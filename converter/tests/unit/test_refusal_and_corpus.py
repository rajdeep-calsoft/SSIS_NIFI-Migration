"""Refusing bad input, and holding the whole corpus to the same bar.

A converter that half-parses a file it does not understand is worse than one
that stops: it produces a flow that looks plausible and is wrong.  Both
negative fixtures come from the colleague's `dtsx_builder.py`, which emits XML
that imitates DTSX without being it, and both must be refused with a message
that names the actual problem.
"""

from __future__ import annotations

import pytest

from ssis2nifi.__main__ import main
from ssis2nifi.dtsx.parse import NotADtsxPackage, parse_file

from .conftest import CORPUS, analyze, real_packages


def test_generated_lookalike_is_refused_on_namespace():
    with pytest.raises(NotADtsxPackage) as exc:
        parse_file(str(CORPUS / "negative_synthetic_wellformed.dtsx"))
    msg = str(exc.value)
    assert "http://www.microsoft.com/SqlServer/Dts" in msg
    assert "not produced by SQL Server Data Tools" in msg


def test_malformed_xml_is_refused_as_xml_not_as_dtsx():
    """This fixture has an unescaped '&' -- it is not even well-formed XML."""
    with pytest.raises(NotADtsxPackage) as exc:
        parse_file(str(CORPUS / "negative_synthetic.dtsx"))
    assert "not well-formed XML" in str(exc.value)


@pytest.mark.parametrize("path", real_packages(), ids=lambda p: p.name)
def test_every_real_package_parses(path):
    pkg = parse_file(str(path))
    assert pkg.name
    assert pkg.sha256
    assert pkg.dialect in {"friendly", "guid", "mixed"}


@pytest.mark.parametrize("path", real_packages(), ids=lambda p: p.name)
def test_every_real_package_has_a_wired_dataflow(path):
    pkg = parse_file(str(path))
    assert pkg.dataflows, "a real package should contain at least one pipeline task"
    for df in pkg.dataflows:
        if len(df.components) > 1:
            assert df.edges, f"{df.name} has components but no edges"


@pytest.mark.parametrize("path", real_packages(), ids=lambda p: p.name)
def test_coverage_accounts_for_every_component(path):
    pkg = analyze(path)
    c = pkg.coverage
    assert c.recognised + c.unsupported == c.total_components


def test_script_components_matching_the_recognised_idiom_convert():
    """L4 and L6 both contain a Script Component that is exactly Microsoft's
    documented GetErrorDescription idiom (catalog/script_pattern.py) -- both
    now convert, narrowed from a blanket refusal the same way Sort/Aggregate
    were (catalog/support.py's NARROW)."""
    for name in ("L4.dtsx", "L6.dtsx"):
        pkg = analyze(CORPUS / name)
        assert pkg.coverage.unsupported == 0, (
            name, [d.message for d in pkg.diagnostics if d.code == "COMPONENT_REFUSED"]
        )


def test_script_components_outside_the_idiom_are_still_refused_by_name_not_crashed_on():
    """The narrowing above is per-instance, not per-class: a Script Component
    whose body isn't exactly the recognised idiom must still be refused,
    named, and not crash the analyzer -- constructed directly (like
    test_expr.py tests catalog/expr.py's bounded grammar with hand-written
    strings) since no real corpus file contains an unrecognised script."""
    from ssis2nifi.catalog.support import classify
    from ssis2nifi.ir.model import Component

    comp = Component(
        id="x", ref_id="Package\\DF\\Arbitrary Script",
        class_id="Microsoft.ManagedComponentHost",
        raw_class_id="Microsoft.ManagedComponentHost",
        name="Arbitrary Script",
        properties={"SourceCode_files": {"main.cs": (
            "public override void Input0_ProcessInputRow(Input0Buffer Row)\n"
            "{\n"
            "    Row.Total = Row.Qty * Row.Price;\n"
            "}\n"
        )}},
    )
    verdict, reason = classify(comp)
    assert verdict == "refused"
    assert "Script Component" in reason


def test_foreach_container_is_reported_not_silently_dropped():
    pkg = analyze(CORPUS / "L2.dtsx")
    codes = {d.code for d in pkg.diagnostics}
    assert "CONTROL_FLOW_CONTAINER" in codes
    # the container is still present as a task, so execution order is recoverable
    assert any(t.kind == "container" for t in pkg.tasks)


# --- exit codes are a contract, not a convenience -------------------------

@pytest.mark.parametrize(
    "name,expected",
    [
        ("L1.dtsx", 0),
        ("L1_guid_dialect.dtsx", 0),
        ("L4.dtsx", 0),                          # Script Component matches the recognised idiom
        ("negative_synthetic_wellformed.dtsx", 4),
        ("negative_synthetic.dtsx", 4),
    ],
)
def test_cli_exit_codes(name, expected, capsys):
    code = main(["analyze", str(CORPUS / name), "--no-graph"])
    capsys.readouterr()
    assert code == expected


def test_json_output_is_valid_ir(capsys):
    import json

    assert main(["analyze", str(CORPUS / "L1.dtsx"), "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["apiVersion"] == "ssis-ir/v1"
    assert doc["dataflows"][0]["edges"]
    assert doc["sha256"]
