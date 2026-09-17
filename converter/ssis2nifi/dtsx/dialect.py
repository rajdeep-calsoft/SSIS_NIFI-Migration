"""SSIS version dialects: the same component, spelled two different ways.

`componentClassID` identifies what a data flow component *is*.  Packages
authored before SQL Server 2012 spell it as a COM CLSID; later ones use a
friendly name:

    SSIS 2008   componentClassID="{671046B0-AA63-4C9F-90E4-C06E0B710CE3}"
    SSIS 2012+  componentClassID="Microsoft.Lookup"

A converter that only understands one dialect silently fails on half the
packages in a real estate, so everything downstream of the parser works in
canonical names and this module is the only place the difference exists.

HOW THE TABLE WAS BUILT -- and how to extend it
-----------------------------------------------
Every entry here was derived by *observation*, not from documentation: the
corpus contains `L1.dtsx` and `L1_guid_dialect.dtsx`, which are the same
logical package saved from two SSIS versions.  Lining up the components by
name gives the mapping directly, and `tests/unit/test_dialect.py` re-derives
it from those two files so the table cannot rot.

To add a GUID: find a package pair that contains the component in both
dialects and let the test derive it.  Do NOT guess a CLSID from a web search
-- a wrong GUID here mistranslates a component rather than refusing it, which
is the one failure mode this tool exists to avoid.
"""

from __future__ import annotations

# GUID -> canonical (friendly) name.
# Verified against corpus/packages/L1.dtsx vs L1_guid_dialect.dtsx.
GUID_TO_NAME: dict[str, str] = {
    "{D23FD76B-F51D-420F-BBCB-19CBF6AC1AB4}": "Microsoft.FlatFileSource",
    "{671046B0-AA63-4C9F-90E4-C06E0B710CE3}": "Microsoft.Lookup",
    "{4ADA7EAA-136C-4215-8098-D7A7C27FC0D1}": "Microsoft.OLEDBDestination",
}

NAME_TO_GUID: dict[str, str] = {v: k for k, v in GUID_TO_NAME.items()}


def canonical(class_id: str) -> str:
    """Canonical component name for either dialect.

    An unknown GUID is returned unchanged rather than guessed at.  It will not
    match a catalogue recipe, so it surfaces as `unsupported` in the coverage
    report -- which is the correct outcome: loud, named, and not translated.
    """
    if not class_id:
        return ""
    key = class_id.strip()
    if key.startswith("{"):
        return GUID_TO_NAME.get(key.upper(), key)
    return key


def is_guid_dialect(class_id: str) -> bool:
    return class_id.strip().startswith("{")


def short(class_id: str) -> str:
    """`Microsoft.Lookup` -> `Lookup`, for report columns that must stay narrow."""
    name = canonical(class_id)
    return name.split(".", 1)[1] if name.startswith("Microsoft.") else name
