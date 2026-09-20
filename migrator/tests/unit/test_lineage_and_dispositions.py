"""The two things that are silently wrong if the parser is subtly wrong.

LINEAGE. DTSX identifies an input column by the refId of the output column
that produced it, and the producer is frequently not the component immediately
upstream.  In L1 the destination's `CurrencyKey` is produced by
`Lookup Currency Key`, reaching back past `Lookup Date Key`.  A parser that
assumes "previous component" wires the wrong field and nothing complains.

DISPOSITIONS. An unconnected output is not "nothing happens".  A Lookup with
`NoMatchBehavior=0` and no path on its no-match output FAILS THE DATA FLOW on
a miss.  Generating a NiFi flow that auto-terminates `unmatched` would silently
discard those rows instead: identical row counts on the happy path, opposite
behaviour when it matters.  These tests pin the disposition down so the
converter cannot quietly choose the wrong one.
"""

from __future__ import annotations


def _component(pkg, name):
    for df in pkg.dataflows:
        for c in df.components:
            if c.name == name:
                return c
    raise AssertionError(f"no component named {name!r}")


def test_lineage_resolves_across_a_non_adjacent_component(l1):
    """CurrencyKey reaches the destination from two components upstream."""
    dest = _component(l1, "Sample OLE DB Destination")
    resolved = {
        col.name: col.lineage_resolved
        for port in dest.inputs
        for col in port.columns
        if col.lineage_resolved
    }
    assert resolved, "destination input columns resolved to nothing"

    assert resolved["CurrencyKey"]["node"] == "lookup_currency_key"
    assert resolved["CurrencyKey"]["output"] == "Lookup Match Output"

    # ...while DateKey comes from the other lookup, the adjacent one
    assert resolved["DateKey"]["node"] == "lookup_date_key"


def test_every_input_column_with_a_lineage_id_resolves(l1):
    unresolved = [
        (c.name, col.name)
        for df in l1.dataflows
        for c in df.components
        for port in c.inputs
        for col in port.columns
        if col.lineage_id and col.lineage_resolved is None
    ]
    assert not unresolved, f"unresolved lineage: {unresolved}"


def test_no_match_output_with_nomatchbehavior_zero_fails_the_dataflow(l1):
    """The silent-row-loss trap. This disposition must never read 'ignore'."""
    lookup = _component(l1, "Lookup Currency Key")
    assert lookup.properties["NoMatchBehavior"] == "0"

    dangling = {
        (d.node, d.output): d for df in l1.dataflows for d in df.dangling_outputs
    }
    d = dangling[("lookup_currency_key", "Lookup No Match Output")]
    assert d.semantics == "no_match"
    assert d.disposition == "fail_component"


def test_error_outputs_are_detected_as_error_semantics(l1):
    lookup = _component(l1, "Lookup Currency Key")
    by_name = {p.name: p for p in lookup.outputs}
    assert by_name["Lookup Error Output"].is_error_out
    assert by_name["Lookup Error Output"].semantics == "error"
    assert by_name["Lookup Match Output"].semantics == "match"
    assert by_name["Lookup No Match Output"].semantics == "no_match"


def test_every_output_is_either_wired_or_recorded_as_dangling(l1):
    """Nothing may be silently dropped from the graph."""
    for df in l1.dataflows:
        wired = {(e.from_node, e.from_output) for e in df.edges}
        dangling = {(d.node, d.output) for d in df.dangling_outputs}
        for comp in df.components:
            for port in comp.outputs:
                key = (comp.id, port.name)
                assert key in wired or key in dangling, f"{key} is accounted for nowhere"
                assert not (key in wired and key in dangling)
