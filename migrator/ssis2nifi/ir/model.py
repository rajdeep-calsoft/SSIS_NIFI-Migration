"""The intermediate representation: what an SSIS package means, as a graph.

WHY THIS IS NOT `NIFI-FLOW/spec/pipeline.yml`
---------------------------------------------
That file is a flat, ordered list of stages with no edges -- the happy path is
implied by list order and the branching is implied by convention.  It is also
*generated from* a NiFi flow, so it is an output, not an input.

A DTSX is a two-level graph: a control-flow DAG of tasks, and inside each
pipeline task an independent data-flow DAG whose edges carry names that mean
something (`Lookup Match Output` vs `Lookup No Match Output`).  Flattening that
into a list loses exactly the information both SSIS and NiFi carry, so the IR
is a graph at both levels.

THE TWO FIELDS THAT LOOK MINOR AND ARE NOT
------------------------------------------
`OutputColumn.lineage_id` -- DTSX wires columns by the refId of the column that
*produced* them, which is frequently NOT the immediately upstream component.
In the corpus, the destination's `CurrencyKey` comes from `Lookup Currency Key`,
reaching past `Lookup Date Key` to get there.  Nothing downstream can be
generated correctly without resolving these.

`DataFlow.dangling_outputs` -- an output with no path attached is not "nothing
happens".  A `Lookup No Match Output` left unconnected with `NoMatchBehavior=0`
means SSIS *fails the entire data flow* on a miss.  Auto-terminating the
equivalent NiFi relationship would silently drop those rows instead: same row
count on the happy path, opposite behaviour when it matters, and nothing to
notice it.  Recording dispositions is how that stays a decision rather than an
accident.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any

IR_API_VERSION = "ssis-ir/v1"


@dataclass
class Diagnostic:
    """Something a human needs to know about this conversion.

    `severity` is one of info | warn | error.  An `error` downgrades the
    package to manual review; it never silently degrades the output.
    """

    severity: str
    code: str
    message: str
    node: str | None = None


@dataclass
class ConnectionManager:
    id: str
    ref_id: str
    kind: str                      # OLEDB | FLATFILE | ADONET | FILE | ...
    creation_name: str = ""
    properties: dict[str, str] = field(default_factory=dict)
    # FLATFILE only: DTS:FlatFileColumns. The per-column delimiters live here,
    # not on the component, and the LAST column's delimiter is really the row
    # delimiter -- see catalog/derive.py.
    columns: list[dict[str, str]] = field(default_factory=list)


@dataclass
class OutputColumn:
    name: str
    ssis_type: str = ""
    ref_id: str = ""
    lineage_id: str = ""
    # For a destination input column: the column in the TARGET table. This is
    # what a column map is built from when the names differ.
    external_metadata_id: str = ""
    error_row_disposition: str = ""
    truncation_row_disposition: str = ""
    # Column-level <properties>. Not decoration: a Lookup expresses its JOIN
    # here (JoinToReferenceColumn on an input column) and which reference
    # columns it returns (CopyFromReferenceColumn on an output column). The
    # component-level properties of the same names are empty.
    properties: dict[str, str] = field(default_factory=dict)
    # filled in by the lineage resolver: which component/output actually produced this
    lineage_resolved: dict[str, str] | None = None


@dataclass
class Port:
    """An input or output of a data flow component."""

    name: str
    ref_id: str
    kind: str                      # input | output
    is_error_out: bool = False
    semantics: str = "success"     # success | match | no_match | error
    columns: list[OutputColumn] = field(default_factory=list)
    # <output><properties> -- e.g. ConditionalSplit's FriendlyExpression per
    # branch. Not a column property: it describes the OUTPUT itself.
    properties: dict[str, str] = field(default_factory=dict)


@dataclass
class Component:
    id: str
    ref_id: str
    class_id: str                  # canonical, dialect already resolved
    raw_class_id: str              # exactly as spelled in the XML
    name: str
    description: str = ""
    uses_dispositions: bool = False
    properties: dict[str, Any] = field(default_factory=dict)
    connections: dict[str, str] = field(default_factory=dict)
    inputs: list[Port] = field(default_factory=list)
    outputs: list[Port] = field(default_factory=list)
    designer: dict[str, float] | None = None
    # Conclusions drawn from `properties` by catalog/derive.py. Recorded next
    # to the raw values so a reviewer can check the reasoning, not just the
    # answer -- e.g. reference_table alongside the SqlCommand it came from.
    derived: dict[str, Any] = field(default_factory=dict)


@dataclass
class Edge:
    """One `<path>` -- a data flow connection with a named, meaningful output."""

    id: str
    ref_id: str
    from_node: str
    from_output: str
    to_node: str
    to_input: str
    semantics: str = "success"


@dataclass
class DanglingOutput:
    """An output with no path attached. See the module docstring."""

    node: str
    output: str
    semantics: str
    disposition: str               # what SSIS does: fail_component | redirect | ignore | unknown


@dataclass
class DataFlow:
    id: str
    ref_id: str
    name: str
    components: list[Component] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    dangling_outputs: list[DanglingOutput] = field(default_factory=list)


@dataclass
class Task:
    """A control-flow executable: a pipeline task, an Execute SQL task, a container."""

    id: str
    ref_id: str
    name: str
    executable_type: str
    kind: str                      # data_flow | execute_sql | container | other
    dataflow: str | None = None
    disabled: bool = False
    properties: dict[str, str] = field(default_factory=dict)


@dataclass
class PrecedenceConstraint:
    from_task: str
    to_task: str
    value: str = "Success"
    expression: str = ""


@dataclass
class Coverage:
    total_components: int = 0
    recognised: int = 0
    unsupported: int = 0

    @property
    def all_recognised(self) -> bool:
        return self.unsupported == 0


@dataclass
class Package:
    """One parsed .dtsx."""

    source_file: str
    sha256: str
    name: str
    creation_name: str = ""
    package_format_version: str = ""
    dts_product_version: str = ""
    dialect: str = ""              # friendly | guid | mixed
    connections: list[ConnectionManager] = field(default_factory=list)
    variables: list[dict[str, str]] = field(default_factory=list)
    tasks: list[Task] = field(default_factory=list)
    precedence: list[PrecedenceConstraint] = field(default_factory=list)
    dataflows: list[DataFlow] = field(default_factory=list)
    diagnostics: list[Diagnostic] = field(default_factory=list)
    coverage: Coverage = field(default_factory=Coverage)

    def to_dict(self) -> dict:
        d = asdict(self)
        d = {"apiVersion": IR_API_VERSION, **d}
        return d

    def diag(self, severity: str, code: str, message: str, node: str | None = None) -> None:
        self.diagnostics.append(Diagnostic(severity, code, message, node))
