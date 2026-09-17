"""SSIS Simulator - shared run/audit helpers.

This module is the SSIS runtime library consumed by the STREAMING runner
(ssis_sim/stream_runner.py). It provides the DB plumbing, audit/run-log/DLQ
writes and the fact-load package (pkg_fact_load) that both the simulator and
the .dtsx specs use.

Package mapping (the .dtsx specs describe the same transforms):
  pkg_ingest_stage     -> stream_runner.ingest_stream_file
  pkg_cleanse_validate -> stream_runner.cleanse_stream
  pkg_dim_load         -> stream_runner.load_dims_stream
  pkg_fact_load        -> load_fact()                          (this module)
  pkg_reconcile        -> stream_runner.reconcile_stream
  pkg_etl_control      -> stream_runner.process_files / stream_daemon
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

import psycopg2
import psycopg2.extras

log = logging.getLogger("sim")

DB = {
    "host": os.environ.get("PG_HOST", "postgres"),
    "port": int(os.environ.get("PG_PORT", 5432)),
    "user": os.environ.get("PG_USER", "etl_user"),
    "password": os.environ.get("PG_PASS", "etl_pass"),
    "database": os.environ.get("PG_DB", "etl_db"),
}
DATA_DIR = os.environ.get("DATA_DIR", "/data")


def conn():
    return psycopg2.connect(connect_timeout=15, **DB)


def next_batch(cur, target_rows=0):
    cur.execute("INSERT INTO control.etl_batch (target_rows) VALUES (%s) "
                "RETURNING etl_batch_id", (target_rows,))
    return cur.fetchone()[0]


def run_log(cur, batch, pkg, status, rows_in=0, rows_staged=0, rows_clean=0,
            rows_rejected=0, rows_loaded=0, started=None, detail=None):
    cur.execute(
        "INSERT INTO control.etl_run_log (etl_batch_id, package_name, status, "
        "rows_in, rows_staged, rows_clean, rows_rejected, rows_loaded, "
        "started_at, finished_at, duration_ms, detail) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (batch, pkg, status, rows_in, rows_staged, rows_clean, rows_rejected,
         rows_loaded, started, datetime.now(),
         int((datetime.now() - started).total_seconds() * 1000) if started else None,
         psycopg2.extras.Json(detail) if detail else None))
    cur.connection.commit()


def metric(cur, batch, pkg, name, value):
    cur.execute(
        "INSERT INTO control.data_quality_metrics "
        "(etl_batch_id, package_name, metric_name, metric_value) "
        "VALUES (%s,%s,%s,%s)", (batch, pkg, name, value))
    cur.connection.commit()


def dlq_insert(cur, batch, source, src_file, src_row, order_id, err_type,
               detail, payload):
    cur.execute(
        "INSERT INTO control.dlq_errors (etl_batch_id, source, source_file, "
        "source_row_id, order_id, error_type, error_detail, raw_payload) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (batch, source, src_file, src_row, order_id, err_type, detail,
         psycopg2.extras.Json(payload)))


def _parse_int(v):
    if v is None:
        return None, "null"
    s = str(v).strip()
    if not s:
        return None, "null"
    try:
        return int(s), None
    except ValueError:
        return None, f"non-numeric '{s}'"


def _parse_num(v):
    if v is None:
        return None, "null"
    s = str(v).strip().replace(",", "")
    if not s:
        return None, "null"
    try:
        return float(s), None
    except ValueError:
        return None, f"non-numeric '{s}'"


# ---------------------------------------------------------------- fact load
def load_fact(cur, batch):
    """pkg_fact_load: append this batch's clean_sales into the fact table,
    resolving each row to its dimension keys and computing total_price."""
    started = datetime.now()
    cur.execute("DELETE FROM dw.fact_sales WHERE etl_batch_id=%s", (batch,))
    cur.execute("""INSERT INTO dw.fact_sales
                   (order_line_id, order_id, date_key, customer_key, product_key,
                    quantity, unit_price, discount_pct, total_price,
                    load_timestamp, etl_batch_id)
                 SELECT c.order_line_id, c.order_id,
                        to_char(c.order_date, 'YYYYMMDD')::int,
                        cust.customer_key, prod.product_key,
                        c.quantity, c.unit_price, c.discount_pct,
                        round(c.quantity * c.unit_price * (1 - c.discount_pct), 4),
                        now(), %s
                 FROM stage.clean_sales c
                 JOIN dw.dim_customer cust
                       ON lower(btrim(cust.customer_id)) = lower(btrim(c.customer_email))
                      AND cust.is_current
                 JOIN dw.dim_product prod ON upper(prod.product_code) = upper(c.product_code)
                 WHERE c.etl_batch_id=%s""", (batch, batch))
    cur.connection.commit()
    cur.execute("SELECT COUNT(*) FROM dw.fact_sales WHERE etl_batch_id=%s", (batch,))
    n = cur.fetchone()[0]
    run_log(cur, batch, "pkg_fact_load", "SUCCESS", rows_loaded=n, started=started)
    return n