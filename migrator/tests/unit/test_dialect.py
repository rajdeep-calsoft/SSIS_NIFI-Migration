"""The GUID table must be DERIVED from the corpus, never hand-maintained.

`corpus/packages/L1.dtsx` and `L1_guid_dialect.dtsx` are the same logical
package saved from two SSIS versions.  Lining their components up by name
yields the GUID -> friendly-name mapping directly, which means these tests
re-prove `dialect.GUID_TO_NAME` from evidence on every run.

This matters because a wrong GUID is the worst kind of bug this tool can
have: it mistranslates a component instead of refusing it.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

from ssis2nifi.dtsx import dialect

from .conftest import CORPUS

DTS = "{www.microsoft.com/SqlServer/Dts}"


def _components_by_name(path) -> dict[str, str]:
    """{component name: raw componentClassID} for every component in the file."""
    root = ET.parse(path).getroot()
    out: dict[str, str] = {}
    for ex in root.iter(f"{DTS}Executable"):
        obj = ex.find(f"{DTS}ObjectData")
        if obj is None:
            continue
        pipe = obj.find("pipeline")
        if pipe is None:
            continue
        for comp in pipe.findall("components/component"):
            out[comp.get("name", "")] = comp.get("componentClassID", "")
    return out


def test_guid_table_is_derivable_from_the_corpus():
    friendly = _components_by_name(CORPUS / "L1.dtsx")
    guid = _components_by_name(CORPUS / "L1_guid_dialect.dtsx")

    shared = set(friendly) & set(guid)
    assert shared, "the two dialect fixtures share no component names"

    derived = {guid[name]: friendly[name] for name in shared if guid[name].startswith("{")}
    assert derived, "no GUID-spelled components found in the guid-dialect fixture"

    for guid_id, friendly_id in derived.items():
        assert dialect.GUID_TO_NAME.get(guid_id.upper()) == friendly_id, (
            f"{guid_id} should map to {friendly_id}; the table disagrees with the corpus"
        )


def test_unknown_guid_is_returned_unchanged_not_guessed():
    unknown = "{00000000-0000-0000-0000-000000000000}"
    assert dialect.canonical(unknown) == unknown


def test_canonical_is_idempotent():
    for guid, name in dialect.GUID_TO_NAME.items():
        assert dialect.canonical(guid) == name
        assert dialect.canonical(name) == name


def test_short_strips_the_microsoft_prefix():
    assert dialect.short("Microsoft.Lookup") == "Lookup"
    assert dialect.short("STOCK:FOREACHLOOP") == "STOCK:FOREACHLOOP"
