"""Writing the IR out and reading it back.

The IR is the artifact a human reviews, so it has to be readable, stable, and
lossless. Those three pull against each other and the choices here are:

READABLE  Block-style YAML, no anchors, keys in declaration order rather than
          alphabetical -- a reader wants `name` before `properties`, not
          `class_id` before `connections`.

STABLE    The same package always serialises identically. That is what makes
          `git diff` on an IR meaningful, and it is the precondition for the
          golden-file tests. Empty collections are omitted rather than written
          as `[]`, so adding an optional field later does not churn every file.

LOSSLESS  `load(dump(pkg)) == pkg`. Enforced by a test over the whole corpus.
          Without it the IR is a report, not an intermediate representation,
          and the Converter could not be run from a reviewed-and-edited file.

TWO DIGESTS, AND WHY THERE ARE TWO
----------------------------------
`content_digest()` hashes everything the IR says about a package except where
the file came from. Two parses of the same bytes must agree; that is what makes
golden-file tests possible.

`topology_digest()` hashes only the SHAPE: which component classes exist, how
they are wired, and what each edge means. It deliberately discards every
configuration value -- table names, SQL, connection strings, column lists.

The second one exists because of a correction. `corpus/packages/L1.dtsx` and
`L1_guid_dialect.dtsx` were initially assumed to be one package saved from two
SSIS versions. They are not: they are the same Microsoft tutorial lesson
authored against different sample databases, so they legitimately differ in
target table (`[dbo].[NewFactCurrencyRate]` vs `[FactCurrency]`), catalog
(AdventureWorksDW2014 vs 2012) and fast-load options.

What they DO share is structure: the same component classes wired the same way
with the same branch semantics, spelled in two dialects. That is the real
property worth testing -- that a GUID-era package and a friendly-name package
analyse to the same shape -- and `topology_digest()` is the honest way to state
it. Claiming byte-identical IR here would have been claiming something these
fixtures cannot show.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from typing import Any

import yaml

from . import model
from .model import (
    Component,
    ConnectionManager,
    Coverage,
    DanglingOutput,
    DataFlow,
    Diagnostic,
    Edge,
    OutputColumn,
    Package,
    Port,
    PrecedenceConstraint,
    Task,
)


class _Dumper(yaml.SafeDumper):
    """No anchors/aliases: a repeated value must print in full.

    YAML would otherwise emit `&id001` / `*id001` for shared objects, which is
    valid but unreadable to anyone who is not a YAML expert -- and this file's
    whole job is to be read by someone who is not.
    """

    def ignore_aliases(self, data: Any) -> bool:
        return True


def _clean(value: Any) -> Any:
    """Drop empty collections and empty strings, recursively.

    An absent field and a field set to "" mean the same thing here, and writing
    both forms makes diffs noisy for no information gain.
    """
    if isinstance(value, dict):
        out = {k: _clean(v) for k, v in value.items()}
        return {k: v for k, v in out.items() if v not in (None, "", [], {})}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    return value


def to_dict(pkg: Package) -> dict:
    """The IR as plain data, cleaned. Declaration order is preserved."""
    raw = dataclasses.asdict(pkg)
    return {"apiVersion": model.IR_API_VERSION, **_clean(raw)}


def dump(pkg: Package) -> str:
    return yaml.dump(
        to_dict(pkg),
        Dumper=_Dumper,
        sort_keys=False,          # declaration order reads better than alphabetical
        default_flow_style=False,
        width=100,
        allow_unicode=True,
    )


# --- reading back --------------------------------------------------------

def _build(cls, data: dict):
    """Rebuild one dataclass, tolerating fields omitted by _clean()."""
    fields = {f.name for f in dataclasses.fields(cls)}
    return cls(**{k: v for k, v in data.items() if k in fields})


def _columns(items: list) -> list[OutputColumn]:
    return [_build(OutputColumn, c) for c in items]


def _ports(items: list) -> list[Port]:
    out = []
    for p in items:
        port = _build(Port, {k: v for k, v in p.items() if k != "columns"})
        port.columns = _columns(p.get("columns", []))
        out.append(port)
    return out


def from_dict(data: dict) -> Package:
    """Inverse of to_dict(). `load(dump(p)) == p` is tested over the corpus."""
    scalar = {k: v for k, v in data.items()
              if k not in {"apiVersion", "connections", "variables", "tasks",
                           "precedence", "dataflows", "diagnostics", "coverage"}}
    pkg = _build(Package, scalar)

    pkg.connections = [_build(ConnectionManager, c) for c in data.get("connections", [])]
    pkg.variables = list(data.get("variables", []))
    pkg.tasks = [_build(Task, t) for t in data.get("tasks", [])]
    pkg.precedence = [_build(PrecedenceConstraint, p) for p in data.get("precedence", [])]
    pkg.diagnostics = [_build(Diagnostic, d) for d in data.get("diagnostics", [])]
    pkg.coverage = _build(Coverage, data.get("coverage", {}))

    for df_data in data.get("dataflows", []):
        df = _build(DataFlow, {k: v for k, v in df_data.items()
                               if k not in {"components", "edges", "dangling_outputs"}})
        for c_data in df_data.get("components", []):
            comp = _build(Component, {k: v for k, v in c_data.items()
                                      if k not in {"inputs", "outputs"}})
            comp.inputs = _ports(c_data.get("inputs", []))
            comp.outputs = _ports(c_data.get("outputs", []))
            df.components.append(comp)
        df.edges = [_build(Edge, e) for e in df_data.get("edges", [])]
        df.dangling_outputs = [_build(DanglingOutput, d)
                               for d in df_data.get("dangling_outputs", [])]
        pkg.dataflows.append(df)
    return pkg


def load(text: str) -> Package:
    return from_dict(yaml.safe_load(text))


# --- digests --------------------------------------------------------------

# Where the file came from. Not part of what it says.
PROVENANCE_KEYS = {"source_file", "sha256", "dts_product_version"}


def _drop(value: Any, keys: set[str]) -> Any:
    if isinstance(value, dict):
        return {k: _drop(v, keys) for k, v in value.items() if k not in keys}
    if isinstance(value, list):
        return [_drop(v, keys) for v in value]
    return value


def _hash(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode()
    ).hexdigest()


def content_digest(pkg: Package) -> str:
    """Everything the IR says, minus where the file came from.

    Two parses of the same bytes agree. Two different packages do not.
    """
    return _hash(_drop(to_dict(pkg), PROVENANCE_KEYS))


def topology_digest(pkg: Package) -> str:
    """The SHAPE only: component classes, wiring, and what each edge means.

    Configuration is deliberately excluded, so this answers one narrow
    question -- "is this the same pipeline structure?" -- and nothing else.
    Two packages with the same digest may still write to different tables.
    """
    shape = {
        "components": sorted(
            (c.name, c.class_id) for df in pkg.dataflows for c in df.components
        ),
        "edges": sorted(
            (e.from_node, e.from_output, e.to_node, e.to_input, e.semantics)
            for df in pkg.dataflows
            for e in df.edges
        ),
        "dangling": sorted(
            (d.node, d.output, d.semantics, d.disposition)
            for df in pkg.dataflows
            for d in df.dangling_outputs
        ),
    }
    return _hash(shape)
