"""Two SSIS dialects, one pipeline shape.

The tool has to be version-agnostic: every real SSIS estate mixes packages
authored across a decade of SQL Server releases, and a converter that only
understands the current spelling is useless on half of them.

SCOPE, STATED EXACTLY. These two fixtures are the same tutorial lesson, not the
same package -- they target different databases and different destination
tables (see corpus/PROVENANCE.md). So this file compares STRUCTURE only:
component classes, the wiring, and the branch semantics of each edge. The
configuration differences are asserted separately, in test_ir_roundtrip.py, so
that the narrower claim made here cannot quietly widen into an overclaim.
"""

from __future__ import annotations


def _shape(pkg):
    """Everything about a package that must survive a dialect change."""
    return {
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
        "coverage": (pkg.coverage.total_components, pkg.coverage.recognised),
    }


def test_both_dialects_report_the_same_dialect_field(l1, l1_guid):
    assert l1.dialect == "friendly"
    assert l1_guid.dialect == "guid"


def test_both_dialects_produce_identical_graphs(l1, l1_guid):
    assert _shape(l1) == _shape(l1_guid)


def test_guid_components_resolve_to_friendly_classes(l1_guid):
    classes = {c.class_id for df in l1_guid.dataflows for c in df.components}
    assert classes == {
        "Microsoft.FlatFileSource",
        "Microsoft.Lookup",
        "Microsoft.OLEDBDestination",
    }
    # ...and the raw spelling is preserved for auditability
    raw = {c.raw_class_id for df in l1_guid.dataflows for c in df.components}
    assert all(r.startswith("{") for r in raw)
