"""Proves the two changes made to the vendored flowdef.py behave as claimed:
_add_alerts is gone entirely, and the job-run ledger is opt-in, driven by
whatever `ledger:` a job's bindings declare -- never a hardcoded table name.
"""
from __future__ import annotations

from ssis2nifi.emit import flowdef

from .conftest import CORPUS, analyze


def _minimal_bindings(pkg) -> dict:
    """Just enough bindings for `pkg` to convert, built from whatever
    connections the package itself declares -- same shape migrator/bindings.py
    produces, inlined here so this test needs no bindings file on disk."""
    bindings: dict = {}
    for i, conn in enumerate(pkg.connections):
        if conn.kind == "OLEDB":
            bindings[f"db{i}"] = {
                "for": conn.ref_id, "db_type": "PostgreSQL", "identifier_case": "lower",
                "url": "jdbc:postgresql://localhost:5555/test",
                "driver_class": "org.postgresql.Driver",
                "driver_path": "/opt/nifi/drivers/postgresql.jar",
                "user": "test", "password_ref": "TEST_DB_PASSWORD",
            }
        elif conn.kind == "FLATFILE":
            bindings[f"file{i}"] = {"for": conn.ref_id, "directory": "/opt/nifi/data/landing"}
    return bindings


def test_alerts_infrastructure_no_longer_exists():
    assert not hasattr(flowdef, "_add_alerts")


def test_no_ledger_configured_means_no_ledger_processors():
    pkg = analyze(CORPUS / "L1.dtsx")
    flow, _secrets, _notes = flowdef.build(pkg, _minimal_bindings(pkg))
    names = {p["name"] for p in flow["flowContents"]["processors"]}
    assert "Record job start" not in names
    assert "Detect Alerts" not in names and "Load Alerts" not in names


def test_ledger_configured_adds_generic_bookkeeping_processors():
    pkg = analyze(CORPUS / "L1.dtsx")
    bindings = _minimal_bindings(pkg)
    bindings["_ledger"] = {"table": "control.job_run_log", "reject_tables": []}
    flow, _secrets, _notes = flowdef.build(pkg, bindings)
    names = {p["name"] for p in flow["flowContents"]["processors"]}
    assert "Record job start" in names
    assert "Record records loaded" in names
    # Never any orders/ecommerce-specific processor name, whatever the domain.
    assert "Detect Alerts" not in names and "Load Alerts" not in names


def test_ledger_table_name_is_config_driven_not_hardcoded():
    pkg = analyze(CORPUS / "L1.dtsx")
    bindings = _minimal_bindings(pkg)
    bindings["_ledger"] = {"table": "some_other_schema.totally_different_ledger_name",
                            "reject_tables": []}
    flow, _secrets, _notes = flowdef.build(pkg, bindings)
    sql = " ".join(
        p["properties"].get("putsql-sql-statement", "")
        for p in flow["flowContents"]["processors"]
        if p["type"].endswith("PutSQL")
    )
    assert "some_other_schema.totally_different_ledger_name" in sql
    assert "job_runs" not in sql
