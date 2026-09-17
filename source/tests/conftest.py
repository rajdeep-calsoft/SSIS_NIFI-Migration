"""Shared pytest configuration for the streaming SSIS tests.

The tests are deterministic in COUNT (they use the generator's inject
scenarios), never in RNG contents, so assertions target counts and reasons,
not specific rows.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for p in (ROOT, os.path.join(ROOT, "ssis_sim"),
          os.path.join(ROOT, "scripts"),
          os.path.join(ROOT, "orders_stream")):
    if p not in sys.path:
        sys.path.insert(0, p)

os.environ.setdefault("PG_HOST", "postgres")
os.environ.setdefault("PG_PORT", "5432")
os.environ.setdefault("PG_USER", "etl_user")
os.environ.setdefault("PG_PASS", "etl_pass")
os.environ.setdefault("PG_DB", "etl_db")

# The generator (orders_stream/gen) connects via libpq-style PG* env vars,
# so mirror the same target into those so `emit.inject` reaches the SAME
# database the tests are connected to (e.g. the isolated etl_test DB).
os.environ.setdefault("PGHOST", os.environ["PG_HOST"])
os.environ.setdefault("PGPORT", os.environ["PG_PORT"])
os.environ.setdefault("PGDATABASE", os.environ["PG_DB"])
os.environ.setdefault("PGUSER", os.environ["PG_USER"])
os.environ.setdefault("PGPASSWORD", os.environ["PG_PASS"])

import pytest  # noqa: E402
import psycopg2  # noqa: E402

DB = {
    "host": os.environ["PG_HOST"], "port": int(os.environ["PG_PORT"]),
    "user": os.environ["PG_USER"], "password": os.environ["PG_PASS"],
    "database": os.environ["PG_DB"],
}


@pytest.fixture(scope="session")
def db_conn():
    c = psycopg2.connect(connect_timeout=15, **DB)
    c.autocommit = True  # never hold "idle in transaction" across tests
    yield c
    c.close()


@pytest.fixture(scope="session")
def stream_schema(db_conn):
    """Make sure base + streaming reference/control tables exist and are seeded."""
    from apply_sql import apply_sql_path
    for f in ("init_schema.sql", "scd2_customer.sql", "seed_dims.sql",
              "stream_schema.sql"):
        script = os.path.join(ROOT, "scripts", f)
        failed = apply_sql_path(db_conn, script)
        assert not failed, (f, failed)
    # Reset the runtime + dimension "fact tables" so each session starts from a
    # pristine ETL state, even when the database previously served the live
    # pipeline (or an earlier pytest run). Reference tables (public.products,
    # public.customers) stay seeded.
    for table in (  # reset order: fact/detail tables before parents is handled by CASCADE
        "stage.sales_stage", "stage.clean_sales",
        "control.dlq_errors", "control.reconcile_results", "control.stream_file",
        "control.stream_alerts", "control.etl_run_log", "control.etl_batch",
        "control.data_quality_metrics", "control.pipeline_runtime", "control.runtime_history", "dw.fact_sales", "dw.dim_customer",
        "dw.dim_product", "dw.dim_date",
    ):
        cur = db_conn.cursor()
        cur.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE")
        cur.close()
    # refill the static dimensions wiped above
    for f in ("seed_dims.sql",):
        script = os.path.join(ROOT, "scripts", f)
        assert not apply_sql_path(db_conn, script), f
    db_conn.commit()
    return True


@pytest.fixture(scope="session")
def landing(tmp_path_factory):
    """A per-session temp landing dir the generator writes NDJSON into."""
    d = tmp_path_factory.mktemp("landing")
    os.environ["LANDING_DIR"] = str(d)
    return str(d)


@pytest.fixture()
def files_processed(db_conn, landing):
    """Process whatever the test wrote to landing; return stream_runner results."""
    from ssis_sim import stream_runner as R

    def _process():
        pending = R.pending_files(landing)
        assert pending, "no pending NDJSON files to process"
        return R.process_files(pending, landing)

    return _process


@pytest.fixture(autouse=True)
def _fresh_runtime(stream_schema, landing, db_conn):
    """Each test starts from an EMPTY pipeline state: no landing files, no
    stage/fact/DLQ/reconcile/alerts history. Reference tables (products,
    customers) and the static dims stay seeded."""
    for name in os.listdir(landing):
        if name.endswith(".json"):
            os.remove(os.path.join(landing, name))
    tables = (
        "stage.sales_stage", "stage.clean_sales",
        "control.dlq_errors", "control.reconcile_results", "control.stream_file",
        "control.stream_alerts", "control.etl_run_log", "control.etl_batch",
        "control.data_quality_metrics", "control.pipeline_runtime", "control.runtime_history", "dw.fact_sales", "dw.dim_customer",
    )
    cur = db_conn.cursor()
    for table in tables:
        cur.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE")
    cur.close()
    db_conn.commit()
    return True


@pytest.fixture()
def inject(stream_schema, landing):
    """Use the generator's one-shot scenarios to write landing files."""
    import gen.emit as emit

    def _inject(scenario):
        emit.inject(scenario)
        # inject only returns the last path; find the newest files.
        d = landing
        files = sorted(
            (os.path.join(d, n) for n in os.listdir(d)
             if n.endswith(".json")),
            key=os.path.getmtime)
        return files

    return _inject