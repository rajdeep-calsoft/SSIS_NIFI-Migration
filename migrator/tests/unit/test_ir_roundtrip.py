"""The IR must survive a round trip, and must be comparable across dialects.

If `load(dump(pkg))` loses anything, the IR is a report rather than an
intermediate representation: the Converter could not be driven from a file a
human has reviewed and edited, which is the entire reason the IR exists.

The digest tests also pin down a CORRECTION. `L1.dtsx` and
`L1_guid_dialect.dtsx` were initially taken to be the same package saved from
two SSIS versions. They are not -- they are the same Microsoft tutorial lesson
authored against different sample databases, and they genuinely differ in
target table and catalog. What they share is structure. These tests assert the
structural claim (which is true and useful) and assert that the content claim
is false, so the distinction cannot quietly rot back into an overclaim.
"""

from __future__ import annotations

import pytest

from ssis2nifi.ir import schema

from .conftest import CORPUS, analyze, real_packages


@pytest.mark.parametrize("path", real_packages(), ids=lambda p: p.name)
def test_round_trip_is_lossless(path):
    pkg = analyze(path)
    once = schema.dump(pkg)
    twice = schema.dump(schema.load(once))
    assert once == twice


@pytest.mark.parametrize("path", real_packages(), ids=lambda p: p.name)
def test_round_trip_preserves_the_graph(path):
    """Not just the bytes -- the structure has to come back too."""
    pkg = analyze(path)
    back = schema.load(schema.dump(pkg))

    assert schema.topology_digest(back) == schema.topology_digest(pkg)
    assert schema.content_digest(back) == schema.content_digest(pkg)
    assert len(back.dataflows) == len(pkg.dataflows)
    for df_a, df_b in zip(pkg.dataflows, back.dataflows):
        assert len(df_a.components) == len(df_b.components)
        assert len(df_a.edges) == len(df_b.edges)
        assert len(df_a.dangling_outputs) == len(df_b.dangling_outputs)


@pytest.mark.parametrize("path", real_packages(), ids=lambda p: p.name)
def test_parsing_is_deterministic(path):
    """Same bytes in, same IR out. The precondition for golden-file testing."""
    assert schema.content_digest(analyze(path)) == schema.content_digest(analyze(path))
    assert schema.dump(analyze(path)) == schema.dump(analyze(path))


def test_round_trip_keeps_lineage_resolution():
    """The most easily-lost field, because it is nested three levels deep."""
    pkg = analyze(CORPUS / "L1.dtsx")
    back = schema.load(schema.dump(pkg))

    def resolved(p):
        return {
            (c.name, col.name): col.lineage_resolved
            for df in p.dataflows
            for c in df.components
            for port in c.inputs
            for col in port.columns
            if col.lineage_resolved
        }

    assert resolved(back) == resolved(pkg)
    assert resolved(pkg), "nothing was resolved, so this test proves nothing"


# --- the cross-dialect claim, stated exactly ------------------------------

def test_both_dialects_share_a_topology(l1, l1_guid):
    """The real, defensible property: same shape, two spellings."""
    assert schema.topology_digest(l1) == schema.topology_digest(l1_guid)


def test_but_they_are_NOT_the_same_package(l1, l1_guid):
    """Guards the correction. These fixtures differ in configuration.

    If this ever starts passing as equal, someone has swapped the fixtures and
    the topology test above has quietly become trivial.
    """
    assert schema.content_digest(l1) != schema.content_digest(l1_guid)

    def dest_table(pkg):
        for df in pkg.dataflows:
            for c in df.components:
                if c.class_id == "Microsoft.OLEDBDestination":
                    return c.properties.get("OpenRowset")
        return None

    assert dest_table(l1) != dest_table(l1_guid)


def test_different_packages_have_different_topologies():
    """Sanity: the digest is not a constant."""
    digests = {
        schema.topology_digest(analyze(CORPUS / name))
        for name in ("L1.dtsx", "L4.dtsx")
    }
    assert len(digests) == 2
