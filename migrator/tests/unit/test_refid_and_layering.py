"""refId parsing, and the architectural boundary that keeps the IR reviewable.

The analyzer must not know NiFi exists.  That is what lets you hand the IR to
the person who owns the SSIS package and ask "is this what it does?" without
them needing to learn a second tool.  The boundary is easy to erode by
accident -- one convenient import -- so it is asserted rather than documented.
"""

from __future__ import annotations

import pathlib
import re

from ssis2nifi.dtsx import refid

SRC = pathlib.Path(__file__).resolve().parents[2] / "ssis2nifi"

REAL = (
    r"Package\Extract Sample Currency Data\Lookup Currency Key"
    r".Outputs[Lookup Match Output].Columns[CurrencyKey]"
)


def test_parses_a_real_column_refid():
    r = refid.parse(REAL)
    assert r.path == ("Package", "Extract Sample Currency Data", "Lookup Currency Key")
    assert r.ports == (("Outputs", "Lookup Match Output"), ("Columns", "CurrencyKey"))
    assert r.leaf == "Lookup Currency Key"
    assert r.port("Outputs") == "Lookup Match Output"
    assert r.port("Columns") == "CurrencyKey"
    assert r.port("Inputs") is None


def test_component_of_strips_every_port_suffix():
    assert refid.component_of(REAL) == (
        r"Package\Extract Sample Currency Data\Lookup Currency Key"
    )


def test_a_bare_refid_has_no_ports():
    r = refid.parse(r"Package\Extract Sample Currency Data")
    assert r.ports == ()
    assert r.owner == r"Package\Extract Sample Currency Data"


def test_raw_is_always_preserved():
    """An unparseable refId is data we must not lose."""
    weird = "not a refid at all"
    assert refid.parse(weird).raw == weird
    assert str(refid.parse(REAL)) == REAL


def test_names_containing_dots_and_brackets_survive():
    r = refid.parse(r"Package\DFT\Odd.Name [v2].Outputs[Out]")
    assert r.leaf == "Odd.Name [v2]"
    assert r.port("Outputs") == "Out"


# --- the boundary ---------------------------------------------------------

def _analyzer_code(path: pathlib.Path) -> str:
    """Executable code only: docstrings and comments stripped, and the project's
    own name removed so `ssis2nifi` does not count as a mention of NiFi."""
    text = path.read_text()
    text = re.sub(r'(?s)""".*?"""', "", text)
    text = re.sub(r"#.*", "", text)
    return text.replace("ssis2nifi", "")


def test_the_analyzer_does_not_import_nifi():
    """dtsx/ and ir/ must stay free of NiFi vocabulary.

    This is what lets the IR be reviewed by the person who owns the SSIS
    package without them learning a second tool. It erodes by one convenient
    import, so it is asserted rather than documented.
    """
    offenders = []
    for path in sorted((SRC / "dtsx").glob("*.py")) + sorted((SRC / "ir").glob("*.py")):
        code = _analyzer_code(path).lower()
        for token in ("nifi", "flowcontents", "processor", "putdatabaserecord",
                      "lookuprecord", "controller_service", "relationship"):
            if token in code:
                offenders.append(f"{path.name}: {token}")
    assert not offenders, f"analyzer leaked NiFi concepts: {offenders}"


def test_the_analyzer_does_not_import_the_converter():
    """No module under dtsx/ or ir/ may reach into emit/ or catalog/."""
    offenders = []
    for path in sorted((SRC / "dtsx").glob("*.py")) + sorted((SRC / "ir").glob("*.py")):
        code = _analyzer_code(path)
        for bad in ("from ..emit", "from ..catalog", "import emit", "import catalog"):
            if bad in code:
                offenders.append(f"{path.name}: {bad}")
    assert not offenders, f"analyzer depends on the converter: {offenders}"
