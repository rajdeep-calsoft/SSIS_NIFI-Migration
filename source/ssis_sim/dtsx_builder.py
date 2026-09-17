"""Generate SSIS .dtsx packages for the E2E ETL.

Emitss the same lean-but-valid DTSX XML dialect used by SSIS packages so the
files can be inspected in Visual Studio / BIML-aware tooling and parsed by the
converter pipeline. The transform behaviour is executed by the simulator
(ssis_sim/simulator.py).
"""
from __future__ import annotations

import os
import uuid

NS0 = "http://www.microsoft.com/SqlServer/Dts"
SCHEMA = "http://www.microsoft.com/sqlserver/integrationservices/task/tsxml/0.7"
CONN_PG = "Driver={PostgreSQL ANSI};Server=postgres;Port=5432;Database=etl_db;UID=etl_user;PWD=etl_pass;"


def _uid():
    return "{%s}" % str(uuid.uuid4()).upper()


def _dt(name, dtsid, exec_type, object_name=None):
    return (f'  <ns0:Executable ns0:Name="{name}" ns0:DTSID="{dtsid}" '
            f'ns0:LocaleID="-1" ns0:ExecutableType="{exec_type}" '
            f'ns0:ObjectName="{name}">')


def _xml_escape(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;")
            .replace("\n", " ").replace("\r", " "))


def _sql_task(name, sql):
    return (f'{_dt(name, _uid(), "Microsoft.ExecuteSQLTask")}\n'
            f'    <ns0:ObjectData>\n'
            f'      <ns1:ExecuteSQLTask xmlns:ns1="{NS0}/tasks" Connection="PG_OLEDB" '
            f'SqlStatementSource="{_xml_escape(sql)}"/>\n'
            f'    </ns0:ObjectData>\n'
            f'  </ns0:Executable>')


def _script_task(name):
    return (f'{_dt(name, _uid(), "ScriptTask")}\n'
            f'    <ns0:ObjectData>\n'
            f'      <ns1:ScriptTask xmlns:ns1="{NS0}/tasks" ScriptLanguage="C#"/>'
            f'\n    </ns0:ObjectData>\n  </ns0:Executable>')


def _foreeach_file(name, vars):
    return f'{_dt(name, _uid(), "ForeachLoop")}\n  </ns0:Executable>'


def _dft(name, parts):
    """Data Flow Task with a list of (kind, props) components."""
    comps = []
    for kind, props in parts:
        comps.append(f'      <ns2:component ns0:Name="{props.get("name", kind)}" '
                     f'ns0:className="{props.get("cls", kind)}" ns0:description="" ns0:componentClassID="">')
        comps.append(f'        <ns2:Properties>')
        for k, v in props.items():
            if k in ("name", "cls"):
                continue
            comps.append(f'          <ns2:property ns0:Name="{k}">{_xml_escape(v)}</ns2:property>')
        comps.append(f'        </ns2:Properties>')
        comps.append(f'      </ns2:component>')

    pipeline = (f'  <ns0:Executable ns0:Name="{name}" ns0:DTSID="{_uid()}" ns0:LocaleID="-1" '
                f'ns0:ExecutableType="SSIS.Pipeline" ns0:ObjectName="{name}">\n'
                f'    <ns0:ObjectData>\n'
                f'      <ns2:Pipeline xmlns:ns2="{NS0}/tasks" ns0:Name="{name}">')
    pipeline += "\n".join(comps)
    pipeline += ('\n      </ns2:Pipeline>\n    </ns0:ObjectData>\n  </ns0:Executable>')
    return pipeline


PACKAGES = {}


def _register(fn):
    PACKAGES[fn.__name__] = fn
    return fn


@_register
def pkg_ingest_stage():
    """Streaming source swap: the generator drops continuous NDJSON order-line
    batches into the landing directory; this package picks each new file up
    (never truncates) and archives every line into stage.sales_stage."""
    parts = [
        ("JsonSource", {
            "name": "Streaming NDJSON Batch Source", "cls": "JsonSource",
            "fileName": "@[User::SourceFilePath]",
            "recordType": "order_line",
            "columns": "order_id|line_no|customer_id|order_ts|order_ts_iso|status|channel|payment_type|currency|sku|qty|unit_price|line_total",
        }),
        ("DerivedColumn", {
            "name": "Add Source Audit", "cls": "DerivedColumn",
            "Expression": "@[User::SourceFilePath]", "NewName": "source_file",
        }),
        ("ConditionalSplit", {
            "name": "Route: Valid JSON / Reader Failure",
            "cls": "ConditionalSplit",
            "Output0": "Valid NDJSON line",
            "Output1": "Quarantine whole batch (READER_FAILURE)",
        }),
        ("OleDbDestination", {
            "name": "Stage Load (append)", "cls": "OleDbDestination",
            "openRowset": "stage.sales_stage",
        }),
    ]
    return _package("pkg_ingest_stage", [
        _foreeach_file("ForEach Pending NDJSON Batch",
                       "User::SourceFilePath"),
        _dft("DFT Ingest Streaming Batch", parts),
        _sql_task("Count Staged Lines",
                  'SELECT COUNT(*) FROM stage.sales_stage '
                  'WHERE source_file = @[User::SourceFilePath]'),
    ], variables=[
        ("SourceFilePath", "/data/landing"),
    ])


@_register
def pkg_cleanse_validate():
    parts = [
        ("OleDbSource", {
            "name": "Sales Stage Source", "cls": "OleDbSource",
            "sqlCommand": "SELECT stage_row_id, order_id, customer_name, customer_email, product_code, quantity, unit_price, discount_pct, order_date, ship_date, notes FROM stage.sales_stage",
        }),
        ("ConditionalSplit", {
            "name": "Route: Valid / Cleanable / Reject",
            "cls": "ConditionalSplit",
            "Output0": "Valid rows",
            "Output1": "Cleanable rows",
            "Output2": "Reject rows",
        }),
        ("Lookup", {
            "name": "Product Catalog Lookup", "cls": "Lookup",
            "sqlCommand": "SELECT sku AS product_code, product_name, category, unit_price AS base_price, manufacturer FROM public.products",
            "errorHandling": "RedirectToErrorOutput",
        }),
        ("DerivedColumn", {
            "name": "Clean + Compute Total",
            "cls": "DerivedColumn",
            "Expression": "LTRIM(RTRIM(customer_name))|[TRIM] category=[LOWER] total_price=quantity*unit_price*(1-discount_pct)",
        }),
        ("SortRemoveDuplicates", {
            "name": "Sort & De-Dupe on order_line", "cls": "SortRemoveDuplicates",
            "keys": "order_id|line_no",
        }),
        ("OleDbDestination", {
            "name": "DLQ Writer", "cls": "OleDbDestination",
            "openRowset": "control.dlq_errors", "errorHandling": "DeadLetter",
        }),
    ]
    return _package("pkg_cleanse_validate", [
        _sql_task("Reset DLQ",
                  "DELETE FROM control.dlq_errors "
                  "WHERE etl_batch_id = @[User::EtlBatchId]"),
        _dft("DFT Cleanse Validate", parts),
    ], variables=[("EtlBatchId", "0")])


@_register
def pkg_dim_load():
    return _package("pkg_dim_load", [
        _sql_task(
            "Seed DimDate",
            "INSERT INTO dw.dim_date SELECT to_char(d,'YYYYMMDD')::int, d, "
            "EXTRACT(YEAR FROM d)::smallint, EXTRACT(QUARTER FROM d)::smallint, "
            "EXTRACT(MONTH FROM d)::smallint, to_char(d,'FMMonth'), "
            "to_char(d,'FMDay'), EXTRACT(DAY FROM d)::smallint, "
            "EXTRACT(DOW FROM d) IN (0,6), false FROM generate_series("
            "'2020-01-01'::date,'2026-12-31'::date,'1 day'::interval) g(d) "
            "ON CONFLICT DO NOTHING"),
        _sql_task("Seed DimProduct",
                  "INSERT INTO dw.dim_product (product_code, product_name, "
                  "category, base_price, manufacturer, is_active) "
                  "SELECT sku, product_name, LOWER(category), unit_price, "
                  "manufacturer, TRUE FROM public.products "
                  "ON CONFLICT (product_code) DO NOTHING"),
        _sql_task("Load DimCustomer SCD2",
                  "CALL dw.upsert_customer(@[User::EtlBatchId])"),
    ], variables=[("EtlBatchId", "0")])


@_register
def pkg_fact_load():
    parts = [
        ("OleDbSource", {
            "name": "Clean Sales Source", "cls": "OleDbSource",
            "sqlCommand": "SELECT order_id, date_key, customer_key, product_key, quantity, unit_price, discount_pct, total_price FROM stage.clean_sales",
        }),
        ("OleDbDestination", {
            "name": "Fact Sales", "cls": "OleDbDestination",
            "openRowset": "dw.fact_sales",
        }),
    ]
    return _package("pkg_fact_load", [
        _sql_task("Reset Fact for Batch",
                  "DELETE FROM dw.fact_sales "
                  "WHERE etl_batch_id = @[User::EtlBatchId]"),
        _dft("DFT Fact Load", parts),
    ], variables=[("EtlBatchId", "0")])


@_register
def pkg_reconcile():
    return _package("pkg_reconcile", [
        _sql_task("Reconcile counts",
                  "INSERT INTO control.etl_run_log (package_name, status, detail) "
                  "SELECT 'pkg_reconcile', CASE WHEN (SELECT COUNT(*) FROM "
                  "stage.clean_sales) = (SELECT COUNT(*) FROM dw.fact_sales) THEN "
                  "'PASS' ELSE 'FAIL' END, jsonb_build_object()"),
    ], variables=[])


@_register
def pkg_etl_control():
    return _package("pkg_etl_control", [
        _sql_task("Start Run",
                  "INSERT INTO control.etl_run_log (package_name,status,started_at) "
                  "VALUES ('pkg_etl_control','RUNNING',now())"),
        _foreeach_file("Run Packages", "User::PkgName"),
        _sql_task("End Run", "UPDATE control.etl_run_log SET finished_at=now(), status='SUCCESS' WHERE status='RUNNING'"),
    ], variables=[("PkgName", "pkg_ingest_stage")]
)


def _package(name, executables, variables, connection=CONN_PG):
    exes = "\n".join(executables)
    conn = (f'  <ns0:ConnectionManagers>\n'
            f'    <ns0:ConnectionManager ns0:Name="PG_OLEDB" ns0:DTSID="{_uid()}" ns0:ObjectName="PG_OLEDB">\n'
            f'      <ns0:ObjectData>\n'
            f'        <ns1:ConnectionManager xmlns:ns1="{NS0}/tasks" '
            f'ConnectionString="{connection}" Provider="OLE DB Provider for PostgreSQL"/>\n'
            f'      </ns0:ObjectData>\n'
            f'    </ns0:ConnectionManager>\n'
            f'  </ns0:ConnectionManagers>')
    vars_xml = "  <ns0:Variables>\n" + "\n".join(
        f'    <ns0:Variable ns0:Name="{n}" ns0:Namespace="User">{v}</ns0:Variable>'
        for n, v in variables) + "\n  </ns0:Variables>"
    return (f'<?xml version=\'1.0\' encoding=\'UTF-8\'?>\n'
            f'<ns0:Executable xmlns:ns0="{NS0}" '
            f'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            f'ns0:Name="{name}" ns0:DTSID="{_uid()}" ns0:LocaleID="-1" '
            f'ns0:CreationDate="2026-01-01T00:00:00" '
            f'xsi:schemaLocation="{NS0} {SCHEMA}">\n'
            f'  <ns0:Executables>\n{exes}\n  </ns0:Executables>\n'
            f'{conn}\n'
            f'{vars_xml}\n'
            f'</ns0:Executable>')


def build_all(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    paths = {}
    for name, builder in PACKAGES.items():
        path = os.path.join(out_dir, name + ".dtsx")
        with open(path, "w") as f:
            f.write(builder())
        paths[name] = path
    return paths


if __name__ == "__main__":
    import sys
    build_all(sys.argv[1] if len(sys.argv) > 1 else "/ssis_packages")