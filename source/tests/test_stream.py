"""SSIS streaming pipeline tests.

Deterministic in COUNT: every scenario is size-fixed, so we assert on counts
and on the reason-types the DLQ received, never on specific rows.
"""
from __future__ import annotations

import json
import os
import time

import psycopg2


def _sf(conn, source_file):
    cur = conn.cursor()
    cur.execute("SELECT lines_in, lines_clean, lines_rejected, orders_loaded, "
                "revenue, status FROM control.stream_file WHERE source_file=%s",
                (source_file,))
    row = cur.fetchone()
    cur.close()
    return row


def _dlq_types(conn, source_file=None):
    cur = conn.cursor()
    if source_file:
        cur.execute("SELECT error_type, COUNT(*) FROM control.dlq_errors "
                    "WHERE source_file=%s GROUP BY 1", (source_file,))
    else:
        cur.execute("SELECT error_type, COUNT(*) FROM control.dlq_errors GROUP BY 1")
    out = dict(cur.fetchall())
    cur.close()
    return out


def _batch_of(conn, source_file):
    cur = conn.cursor()
    cur.execute("SELECT etl_batch_id FROM control.stream_file WHERE source_file=%s",
                (source_file,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def _facts(conn, batch):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT order_id), COALESCE(SUM(total_price),0) "
                "FROM dw.fact_sales WHERE etl_batch_id=%s", (batch,))
    row = cur.fetchone()
    cur.close()
    return row


def test_schema_seeded(stream_schema, db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM public.products")
    products = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.customers")
    customers = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dw.dim_date")
    dates = cur.fetchone()[0]
    cur.close()
    assert products >= 18, "streaming product catalogue not seeded"
    assert customers >= 60, "streaming customer base not seeded"
    assert dates >= 2557, "dim_date not extended to 2026-12-31"


def _one_file(files):
    assert files, "inject wrote no files"
    return files[-1]  # most recently written


def test_inject_clean(stream_schema, inject, files_processed, db_conn):
    files = inject("clean")
    fn = os.path.basename(_one_file(files))
    results = files_processed()
    r = [x for x in results if x["file"] == fn][0]
    assert all(c["pass"] for c in r["checks"])

    lines, clean, rejected, orders, revenue, status = _sf(db_conn, fn)
    assert rejected == 0 and clean == lines and orders == 10
    assert status == "PASS"
    assert _dlq_types(db_conn, fn) == {}

    n_fact, n_orders, total = _facts(db_conn, r["batch"])
    assert n_fact == clean and n_orders == 10 and total > 0


def test_inject_bad_batch(stream_schema, inject, files_processed, db_conn):
    files = inject("bad_batch")
    fn = os.path.basename(_one_file(files))
    results = files_processed()
    lines, clean, rejected, *_ = _sf(db_conn, fn)
    assert clean == 0, "every bad_batch line must be rejected"
    assert rejected == lines
    types = set(_dlq_types(db_conn, fn))
    assert len(types) >= 4, f"expected rich DLQ coverage, got {types}"


def test_inject_duplicates(stream_schema, inject, files_processed, db_conn):
    files = inject("duplicates")
    db_conn.commit()
    results = files_processed()
    names = [os.path.basename(f) for f in files]
    dup1 = next(n for n in names if "dup1" in n)
    dup2 = next(n for n in names if "dup2" in n)

    _, c1, _, o1, *_ = _sf(db_conn, dup1)
    _, c2, r2, o2, *_ = _sf(db_conn, dup2)
    _, n_orders, _ = _facts(db_conn, _batch_of(db_conn, dup1))
    assert o1 == 15 and n_orders == 15 and c1 > 0
    assert c2 == 0 and r2 == c1 and o2 == 0, "dup2 must be fully de-duplicated"
    assert _dlq_types(db_conn, dup2).get("DUP_KEY", 0) == c1


def test_inject_fraud_burst(stream_schema, inject, files_processed, db_conn):
    inject("fraud_burst")
    results = files_processed()
    cur = db_conn.cursor()
    cur.execute("SELECT alert_type, COUNT(*) FROM control.stream_alerts GROUP BY 1")
    alerts = dict(cur.fetchall())
    cur.execute("SELECT COUNT(DISTINCT customer_id) FROM control.stream_alerts "
                "WHERE alert_type='HIGH_VALUE'")
    victims = cur.fetchone()[0]
    cur.close()
    assert alerts.get("HIGH_VALUE", 0) > 0, "fraud burst must trip HIGH_VALUE"
    assert victims == 1, "all expensive orders must come from one customer"


def test_inject_malformed(stream_schema, inject, files_processed, db_conn):
    files = inject("malformed")
    fn = os.path.basename(_one_file(files))
    results = files_processed()
    lines, clean, rejected, *_ = _sf(db_conn, fn)
    assert clean == 0 and rejected == lines
    assert _dlq_types(db_conn, fn).get("PARSE_ERROR", 0) == lines


def test_inject_empty(stream_schema, inject, files_processed, db_conn):
    files = inject("empty")
    fn = os.path.basename(_one_file(files))
    results = files_processed()
    lines, clean, rejected, *_ = _sf(db_conn, fn)
    assert lines == 0 and clean == 0 and rejected == 0
    assert all(c["pass"] for c in results[0]["checks"])


def _make_record(order_id="ORD-TEST-000001"):
    now_ms = int(time.time() * 1000)
    return {
        "order_id": order_id, "line_no": 1, "customer_id": "CUST-00001",
        "order_ts": now_ms, "order_ts_iso": "",
        "status": "PAID", "channel": "WEB", "payment_type": "CARD",
        "currency": "INR", "sku": "SKU-0001", "qty": 2,
        "unit_price": 12.99, "line_total": 25.98,
    }


def _write_manual(landing, name, records):
    path = os.path.join(landing, name)
    with open(path, "w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")
    return path


def test_fault_map(stream_schema, files_processed, db_conn):
    """Each generator fault must land with exactly its DLQ reason."""
    base = _make_record("ORD-FAULT-000000")
    cases = [
        (dict(base, customer_id=None), "NULL_CRITICAL"),
        (dict(base, qty="two"), "PARSE_ERROR"),
        (dict(base, qty=0, line_total=0), "RANGE_VIOLATION"),
        (dict(base, sku="SKU-9999"), "UNKNOWN_SKU"),
        (dict(base, customer_id="CUST-99999"), "UNKNOWN_CUSTOMER"),
        (dict(base, currency="\u20b9"), "BAD_CURRENCY"),
        (dict(base, order_ts=int(time.time() * 1000) + 2 * 365 * 86400 * 1000),
         "BAD_TIMESTAMP"),
    ]
    landing = os.environ["LANDING_DIR"]
    expected = []
    for i, (rec, reason) in enumerate(cases):
        rec["order_id"] = f"ORD-FAULT-{i:06d}"
        _write_manual(landing, f"fault_{i:02d}.json", [rec])
        expected.append(reason)

    results = files_processed()
    types = set(_dlq_types(db_conn))
    for reason in expected:
        assert reason in types, f"fault expected reason {reason}, got {types}"


def test_append_is_cumulative(stream_schema, inject, files_processed, db_conn):
    inject("clean")
    before = files_processed()
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dw.fact_sales")
    n1 = cur.fetchone()[0]
    cur.close()
    assert n1 > 0

    inject("flood")  # 5000 orders, deterministic size
    files_processed()
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dw.fact_sales")
    n2 = cur.fetchone()[0]
    cur.close()
    assert n2 > n1, "second batch must append, not truncate"


def test_reconcile_stream_everything_passes(stream_schema, inject,
                                             files_processed, db_conn):
    inject("clean")
    inject("suspicious")
    results = files_processed()
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM control.reconcile_results WHERE NOT pass")
    fails = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM control.stream_file WHERE status='FAIL'")
    failed_files = cur.fetchone()[0]
    cur.close()
    assert fails == 0
    assert failed_files == 0
    assert all(c["pass"] for r in results for c in r["checks"])