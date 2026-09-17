"""Streaming SSIS runner (SSIS-only swap).

`orders_stream` drops continuous NDJSON
order-line batches into a landing directory and this module picks each new
file up and runs the SAME six-package SSIS flow (pkg_ingest_stage ->
pkg_cleanse_validate -> pkg_dim_load -> pkg_fact_load -> pkg_reconcile) but in
APPEND mode -- nothing is truncated, so the warehouse grows with the stream
and Grafana can plot it live.

Package mapping (mirrors the .dtsx specs):
  pkg_ingest_stage     -> ingest_stream_file()   (read NDJSON batch -> stage)
  pkg_cleanse_validate -> cleanse_stream()       (validate + DLQ + dedup + alerts)
  pkg_dim_load         -> load_dims_stream()     (dims upsert, SCD2 customers)
  pkg_fact_load        -> load_fact_stream()     (append fact rows per batch)
  pkg_reconcile        -> reconcile_stream()     (per-file invariant checks)
  pkg_etl_control      -> process_files() / stream_daemon() (orchestrator)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from . import simulator as S

log = logging.getLogger("stream")

LANDING = os.environ.get("LANDING_DIR", os.path.join(S.DATA_DIR, "landing"))
CURRENCY_OK = {"INR", "USD", "EUR", "GBP"}
TS_WINDOW_DAYS = 365
HIGH_VALUE_MIN = float(os.environ.get("STREAM_HIGH_VALUE_MIN", "500"))
SUSPICIOUS_QTY = 50


def connect():
    return psycopg2_connect()


def psycopg2_connect():
    return S.conn()


# ---------------------------------------------------------------- ingest
def _map_record(d: dict) -> tuple:
    """Map an NDJSON order-line record onto stage.sales_stage columns.

    Returns (stage_values, ok_fields) where ok_fields is None if the JSON is
    structurally impossible, otherwise the raw values for validation.
    """
    order_id = str(d.get("order_id") or "").strip()
    sku = str(d.get("sku") or "").strip()
    cust = str(d.get("customer_id") or "").strip()
    qty = d.get("qty")
    price = d.get("unit_price")
    ts = d.get("order_ts")
    currency = str(d.get("currency") or "").strip()
    return (order_id, sku, cust, qty, price, ts, currency)


def ingest_stream_file(cur, batch, path):
    """pkg_ingest_stage for one NDJSON batch file. Every line is archived in
    stage.sales_stage (parse_status OK / PARSE_ERROR); the raw JSON is kept in
    raw_line + notes so the DLQ can replay it."""
    import json as _json

    started = datetime.now()
    fn = os.path.basename(path)
    staged = bad_json = 0
    products = _product_lookup(cur)
    customers = _customer_lookup(cur)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line_no, raw in enumerate(fh, start=1):
                raw = raw.rstrip("\r\n")
                if not raw.strip():
                    continue
                try:
                    d = _json.loads(raw)
                except (ValueError, TypeError):
                    status, err = "PARSE_ERROR", "invalid json line"
                    d = {}
                else:
                    if not isinstance(d, dict):
                        status, err = "PARSE_ERROR", "json not an object"
                        d = {}
                    else:
                        status, err = "OK", None

                if status == "PARSE_ERROR":
                    bad_json += 1

                (order_id, sku, cust, qty, price, ts,
                 currency) = _map_record(d)

                # lookups for the audit columns (name/email/category)
                name = email = pname = cat = ""
                if sku in products:
                    pname, cat = products[sku]
                if cust in customers:
                    name, email = customers[cust]

                order_date = ""
                try:
                    order_date = datetime.fromtimestamp(
                        int(ts) / 1000, tz=timezone.utc).date().isoformat()
                except (TypeError, ValueError, OSError):
                    pass

                notes = {k: d.get(k) for k in
                         ("line_no", "status", "channel", "payment_type",
                          "currency", "line_total", "order_ts_iso")
                         if k in d}
                cur.execute(
                    """INSERT INTO stage.sales_stage
                       (source_file, source_line_no, order_id, customer_name,
                        customer_email, product_code, product_name, category,
                        quantity, unit_price, discount_pct, order_date, ship_date,
                        notes, raw_line, parse_status, parse_error)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (fn, line_no, order_id, name, email, sku, pname, cat,
                     "" if qty is None else str(qty),
                     "" if price is None else str(price), "0",
                     order_date, order_date,
                     json.dumps(notes) if notes else None, raw, status, err))
                staged += 1
    except OSError as exc:
        _reader_failure(cur, batch, fn, str(exc))
        S.run_log(cur, batch, "pkg_ingest_stage", "FAILED", rows_in=0,
                  rows_staged=0, rows_rejected=1, started=started,
                  detail={"error": str(exc)})
        return {"lines": 0, "staged": 0, "bad_json": 0, "failed": True}

    cur.connection.commit()
    S.run_log(cur, batch, "pkg_ingest_stage", "SUCCESS", rows_in=staged,
              rows_staged=staged, rows_rejected=bad_json, started=started)
    return {"lines": staged, "staged": staged, "bad_json": bad_json,
            "failed": False}


def _reader_failure(cur, batch, fn, err):
    S.dlq_insert(cur, batch, "STREAM", fn, 0, "", "READER_FAILURE",
                 f"file could not be read: {err}",
                 {"source_file": fn, "error": err})


# The ingest loop calls each of these twice per line. Un-cached that is a full
# table scan per call -- 400k scans on a 100k-line file, which dominates the
# run. The reference tables are seeded once and never written during a batch,
# so one read per process is correct as well as very much faster.
_LOOKUP_CACHE: dict[str, dict] = {}


def reset_lookup_cache() -> None:
    """Drop the memoized catalogue. Call after re-seeding the dimensions."""
    _LOOKUP_CACHE.clear()


def _product_lookup(cur):
    if "products" not in _LOOKUP_CACHE:
        cur.execute("SELECT sku, product_name, LOWER(category) FROM public.products")
        _LOOKUP_CACHE["products"] = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    return _LOOKUP_CACHE["products"]


def _customer_lookup(cur):
    if "customers" not in _LOOKUP_CACHE:
        cur.execute("SELECT customer_id, customer_name, email "
                    "FROM public.customers")
        _LOOKUP_CACHE["customers"] = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    return _LOOKUP_CACHE["customers"]


# ---------------------------------------------------------------- cleanse
def _validate_stream(cur, batch, src_file, src_row, d, products, customers,
                     alerts):
    """Validate one stream record against the SSIS quality rules. Mirrors the
    generator's fault catalogue so every fault lands with one exact reason."""
    def reject(err_type, detail):
        S.dlq_insert(cur, batch, "STREAM", src_file, src_row,
                     str(d.get("order_id") or ""), err_type, detail, d)
        return "REJECT"

    order_id = str(d.get("order_id") or "").strip()
    sku = str(d.get("sku") or "").strip()
    cust = str(d.get("customer_id") or "").strip()

    missing = [f for f, x in [("order_id", order_id), ("sku", sku),
                               ("customer_id", cust), ("qty", d.get("qty")),
                               ("unit_price", d.get("unit_price")),
                               ("order_ts", d.get("order_ts"))]
               if x in (None, "")]
    if missing:
        return reject("NULL_CRITICAL", f"missing {missing}")

    # line_no is half the dedupe business key (order_id, line_no) and it was
    # never checked. A wrong_type fault can set it to "two" or "N/A", and that
    # value then travelled all the way into stage.clean_sales.notes and into
    # the dedupe key -- so ("ORD-x", "two") and ("ORD-x", "2") counted as two
    # different order lines. The NiFi engine types line_no as int and rejects
    # it at the schema gate, so this was also the single largest source of
    # disagreement between the two engines (69 records in one run).
    #
    # NOT part of the `missing` list above on purpose: NiFi does not require
    # line_no to be present, it requires it to be an integer. An empty line_no
    # is PARSE_ERROR on both sides, not NULL_CRITICAL.
    _line_no, err = S._parse_int(d.get("line_no"))
    if err:
        return reject("PARSE_ERROR", f"line_no {err}")

    qty, err = S._parse_int(d.get("qty"))
    if err:
        return reject("PARSE_ERROR", f"quantity {err}")
    if not (1 <= qty <= 999):
        return reject("RANGE_VIOLATION", f"quantity {qty}")

    price, err = S._parse_num(d.get("unit_price"))
    if err:
        return reject("PARSE_ERROR", f"unit_price {err}")
    if not (0.01 <= price <= 9999.99):
        return reject("RANGE_VIOLATION", f"unit_price {price}")

    if sku not in products:
        return reject("UNKNOWN_SKU", f"sku '{sku}' not in product catalogue")
    if cust not in customers:
        return reject("UNKNOWN_CUSTOMER", f"customer '{cust}' not in subscribers")

    cur_ts = datetime.now(tz=timezone.utc)
    try:
        ts = int(d.get("order_ts"))
        ots = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return reject("PARSE_ERROR", f"order_ts not a valid epoch ms")
    if abs((cur_ts - ots).total_seconds()) > TS_WINDOW_DAYS * 86400:
        return reject("BAD_TIMESTAMP", f"order_ts {ots.isoformat()} outside "
                                       f"{TS_WINDOW_DAYS}d window")

    currency = str(d.get("currency") or "").strip().upper()
    if currency not in CURRENCY_OK:
        return reject("BAD_CURRENCY", f"currency '{d.get('currency')}'")

    line_total = float(d.get("line_total") or 0)
    computed = round(qty * price, 2)
    fixes = []
    if abs(line_total - computed) > 0.02:
        fixes.append(f"line_total_fixed:{computed}")
    pname, cat = products[sku]
    name, email = customers[cust]

    if line_total > HIGH_VALUE_MIN:
        alerts.append((order_id, cust, "HIGH_VALUE", line_total,
                       {"line_total": line_total, "sku": sku}))
    if qty > SUSPICIOUS_QTY:
        alerts.append((order_id, cust, "SUSPICIOUS_QTY", qty,
                       {"qty": qty, "sku": sku}))

    odate = ots.date()
    cur.execute(
        """INSERT INTO stage.clean_sales
           (etl_batch_id, source, source_file, source_row_id, order_line_id,
            order_id, customer_name, customer_email, product_code, product_name,
            category, quantity, unit_price, discount_pct, order_date, ship_date,
            notes, quality_status, defects, fingerprint)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (batch, "STREAM", src_file, src_row, f"{order_id}#{src_row}",
         order_id, name, email, sku.upper(), pname, cat, qty, round(price, 4),
         0.0, odate, odate,
         json.dumps({"line_no": d.get("line_no"),
                     "channel": d.get("channel"), "status": d.get("status"),
                     "payment_type": d.get("payment_type"),
                     "currency": currency, "line_total": d.get("line_total")},
                    default=str),
         "CLEANED" if fixes else "VALID", json.dumps(fixes) if fixes else None,
         __import__("hashlib").md5(
            (f"{order_id}|{name}|{email}|{sku.upper()}|{qty}|{round(price,4)}|{odate}")
            .encode()).hexdigest()))
    return "CLEAN"


def cleanse_stream(cur, batch, files):
    started = datetime.now()
    products = _product_lookup(cur)
    customers = _customer_lookup(cur)
    alerts = []

    cur.execute("""SELECT DISTINCT order_id, notes::json->>'line_no'
                   FROM stage.clean_sales
                   WHERE order_id IS NOT NULL""")
    existing_keys = {(str(oid) if oid else "", ln) for oid, ln in cur.fetchall()}

    n_in = n_clean = 0

    for fn in files:
        cur.execute("""SELECT stage_row_id, source_file, parse_status, raw_line,
                              order_id, notes
                       FROM stage.sales_stage
                       WHERE source_file=%s ORDER BY source_line_no""", (fn,))
        rows = cur.fetchall()
        for row_id, sfile, pstat, raw, oid, notes_json in rows:
            n_in += 1
            if pstat == "PARSE_ERROR":
                S.dlq_insert(cur, batch, "STREAM", sfile, row_id, oid,
                             "PARSE_ERROR", "invalid json line", {"raw": raw})
                continue
            try:
                d = json.loads(raw)
            except (ValueError, TypeError):
                S.dlq_insert(cur, batch, "STREAM", sfile, row_id, oid,
                             "PARSE_ERROR", "unparseable json", {"raw": raw})
                continue
            if _validate_stream(cur, batch, sfile, row_id, d, products,
                                customers, alerts) == "CLEAN":
                n_clean += 1

    cur.connection.commit()

    # dedupe on the ORDER LINE business key (order_id, line_no): an order's own
    # extra lines are NOT duplicates, but a replayed file (same lines again)
    # is. (1) key already seen in an earlier batch -> DUP_KEY, (2) duplicate
    # line within this batch -> keep lowest source_row_id.
    cur.execute("""SELECT clean_id, order_id, notes::json->>'line_no', source_row_id
                   FROM stage.clean_sales WHERE etl_batch_id=%s""", (batch,))
    winners = {}
    losers = []
    for cid, oid, line_no, srid in cur.fetchall():
        key = (str(oid) if oid else "", line_no)
        if key in existing_keys or key in winners:
            losers.append((cid, oid, srid, line_no))
        else:
            winners[key] = (cid, oid, srid)
    # which DLQ row is attributed to its own file (DUP_KEY losers carry the
    # source file so the Grafana DLQ-by-file view stays accurate).
    src_file = files[0] if len(files) == 1 else None
    for cid, oid, srid, line_no in losers:
        # line_no belongs in the payload: it is half the business key, and the
        # cross-engine reject comparison reads it back out of raw_payload.
        S.dlq_insert(cur, batch, "STREAM", src_file, srid, oid, "DUP_KEY",
                     "duplicate order line on business key (order_id, line_no)",
                     {"order_id": oid, "line_no": line_no})
    for cid, *_ in losers:
        cur.execute("DELETE FROM stage.clean_sales WHERE clean_id=%s", (cid,))
    cur.connection.commit()

    for order_id, cust, atype, value, detail in alerts:
        cur.execute(
            "INSERT INTO control.stream_alerts (etl_batch_id, source_file, "
            "order_id, customer_id, alert_type, alert_value, detail) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (batch, files[0] if len(files) == 1 else None, order_id, cust,
             atype, value, json.dumps(detail)))
    cur.connection.commit()

    cur.execute("SELECT COUNT(*) FROM stage.clean_sales WHERE etl_batch_id=%s",
                (batch,))
    clean_final = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM control.dlq_errors WHERE etl_batch_id=%s",
                (batch,))
    dlq_total = cur.fetchone()[0]

    S.run_log(cur, batch, "pkg_cleanse_validate", "SUCCESS", rows_in=n_in,
              rows_clean=clean_final, rows_rejected=dlq_total, started=started)
    S.metric(cur, batch, "pkg_cleanse_validate", "dlq_errors", dlq_total)
    S.metric(cur, batch, "pkg_cleanse_validate", "alerts", len(alerts))
    return clean_final, dlq_total, len(alerts)


# ---------------------------------------------------------------- dims + fact
def load_dims_stream(cur, batch):
    started = datetime.now()
    cur.execute("""INSERT INTO dw.dim_product (product_code, product_name,
                       category, base_price, manufacturer, is_active, etl_batch_id)
                   SELECT sku, product_name, category, unit_price,
                          COALESCE(manufacturer, 'Stream'), TRUE, %s
                   FROM public.products
                   ON CONFLICT (product_code) DO NOTHING""", (batch,))
    cur.execute("CALL dw.upsert_customer(%s)", (batch,))
    cur.connection.commit()
    S.run_log(cur, batch, "pkg_dim_load", "SUCCESS", started=started)
    return 1


def load_fact_stream(cur, batch):
    return S.load_fact(cur, batch)


# ---------------------------------------------------------------- reconcile
def reconcile_stream(cur, batch, files):
    started = datetime.now()

    def nq(q, *a):
        cur.execute(q, a)
        return cur.fetchone()[0]

    staged = nq("SELECT COUNT(*) FROM stage.sales_stage "
                "WHERE source_file = ANY(%s)", (files,))
    clean = nq("SELECT COUNT(*) FROM stage.clean_sales WHERE etl_batch_id=%s",
               batch)
    dlq = nq("SELECT COUNT(*) FROM control.dlq_errors WHERE etl_batch_id=%s",
             batch)
    fact = nq("SELECT COUNT(*) FROM dw.fact_sales WHERE etl_batch_id=%s", batch)
    orders = nq("SELECT COUNT(DISTINCT order_id) FROM dw.fact_sales "
                "WHERE etl_batch_id=%s", batch)
    revenue = nq("SELECT COALESCE(SUM(total_price),0) FROM dw.fact_sales "
                 "WHERE etl_batch_id=%s", batch)

    checks = [
        ("clean_vs_fact", clean, fact,
         "clean_sales count must equal loaded fact rows"),
        ("sources_vs_outcome", staged, clean + dlq,
         "every staged line must be clean or land in the DLQ"),
    ]
    results = []
    for name, src, tgt, note in checks:
        ok = (src == tgt)
        results.append({"name": name, "pass": ok, "src": src, "tgt": tgt})
        cur.execute(
            "INSERT INTO control.reconcile_results (etl_batch_id, check_name, "
            "source_count, target_count, pass, detail) VALUES (%s,%s,%s,%s,%s,%s)",
            (batch, name, src, tgt, ok, json.dumps({"note": note})))

    # source_file is UNIQUE and doubles as the watermark. Without ON CONFLICT,
    # re-processing a file of the same name raises instead of re-recording it,
    # which makes a fixture impossible to replay.
    cur.execute(
        "INSERT INTO control.stream_file (source_file, etl_batch_id, lines_in, "
        "lines_clean, lines_rejected, orders_loaded, revenue, status) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (source_file) DO UPDATE SET "
        "  etl_batch_id = EXCLUDED.etl_batch_id, "
        "  lines_in = EXCLUDED.lines_in, "
        "  lines_clean = EXCLUDED.lines_clean, "
        "  lines_rejected = EXCLUDED.lines_rejected, "
        "  orders_loaded = EXCLUDED.orders_loaded, "
        "  revenue = EXCLUDED.revenue, "
        "  status = EXCLUDED.status, "
        "  processed_at = now()",
        (files[0], batch, staged, clean, dlq, orders, revenue,
         "PASS" if all(r["pass"] for r in results) else "FAIL"))
    cur.connection.commit()

    S.run_log(cur, batch, "pkg_reconcile", "SUCCESS", rows_in=staged,
              rows_clean=clean, rows_rejected=dlq, rows_loaded=fact,
              started=started)
    S.metric(cur, batch, "pkg_reconcile", "orders_loaded", orders)
    S.metric(cur, batch, "pkg_reconcile", "revenue", int(revenue))
    return results


# ---------------------------------------------------------------- control flow
def existing_files(cur):
    cur.execute("SELECT COALESCE(array_agg(source_file), '{}') FROM control.stream_file")
    return set(cur.fetchone()[0])


def pending_files(landing=LANDING):
    known = None
    c = connect()
    cur = c.cursor()
    known = existing_files(cur)
    cur.close()
    c.close()
    pending = []
    if os.path.isdir(landing):
        for name in sorted(os.listdir(landing)):
            p = os.path.join(landing, name)
            if os.path.isfile(p) and name.endswith(".json") and name not in known:
                pending.append(p)
    return pending


def process_files(paths, landing=LANDING):
    """pkg_etl_control: one batch per source file, packages in order."""
    if not paths:
        return []
    c = connect()
    cur = c.cursor()
    out = []
    try:
        for path in paths:
            started = datetime.now()
            batch = S.next_batch(cur, 0)
            S.run_log(cur, batch, "pkg_etl_control", "STARTED", started=started)

            ingest = ingest_stream_file(cur, batch, path)
            clean, dlq_total, alerts = cleanse_stream(cur, batch, [os.path.basename(path)])
            load_dims_stream(cur, batch)
            fact = load_fact_stream(cur, batch)
            checks = reconcile_stream(cur, batch, [os.path.basename(path)])

            cur.execute("UPDATE control.etl_batch SET finished_at=now() "
                        "WHERE etl_batch_id=%s", (batch,))
            cur.connection.commit()
            out.append({
                "batch": batch, "file": os.path.basename(path),
                "lines_staged": ingest["staged"], "bad_json": ingest["bad_json"],
                "clean": clean, "rejected": dlq_total, "fact": fact,
                "alerts": alerts,
                "checks": [{"name": r["name"], "pass": r["pass"]}
                           for r in checks],
            })
    finally:
        cur.close()
        c.close()
    return out


def stream_status(landing=LANDING):
    """Cumulative totals for reports / Grafana-style summary."""
    c = connect()
    cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM stage.sales_stage")
    staged = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM control.dlq_errors")
    rejected = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dw.fact_sales")
    fact = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT order_id) FROM dw.fact_sales")
    orders = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(total_price),0) FROM dw.fact_sales")
    revenue = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dw.dim_customer WHERE is_current")
    customers = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM dw.dim_product")
    products = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM control.stream_file")
    files = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM control.stream_file WHERE status='FAIL'")
    failed = cur.fetchone()[0]
    cur.close()
    c.close()
    return {"staged": staged, "rejected": rejected, "fact": fact,
            "orders": orders, "revenue": float(revenue),
            "customers_current": customers, "products": products,
            "files_processed": files, "files_failed": failed}


def process_pending(landing=LANDING):
    pending = pending_files(landing)
    results = process_files(pending, landing=landing)
    c = connect()
    try:
        cur = c.cursor()
        update_health(cur, landing)
        cur.close()
    finally:
        c.close()
    return results, len(pending)


def update_health(cur, landing=LANDING):
    """Refresh the singleton ops row Grafana reads (queue depth, heartbeat,
    failed jobs, last error). Never raises -- health must not break the
    pipeline."""
    try:
        known = set()
        cur.execute("SELECT source_file FROM control.stream_file")
        known = {r[0] for r in cur.fetchall()}
        pending = 0
        if os.path.isdir(landing):
            pending = sum(1 for n in os.listdir(landing)
                          if n.endswith(".json") and n not in known)
        cur.execute("""SELECT etl_batch_id, source_file, status
                       FROM control.stream_file
                       ORDER BY processed_at DESC, stream_id DESC LIMIT 1""")
        row = cur.fetchone()
        cur.execute("""SELECT COALESCE(COUNT(*), 0) FROM control.stream_file
                       WHERE status = 'FAIL'""")
        failed = cur.fetchone()[0]
        cur.execute("""SELECT detail->>'error' FROM control.etl_run_log
                       WHERE status = 'FAILED' OR detail->>'error' IS NOT NULL
                       ORDER BY started_at DESC, run_log_id DESC LIMIT 1""")
        err = cur.fetchone()
        cur.execute(
            """INSERT INTO control.pipeline_runtime
               (singleton, as_of, pending_files, last_batch_id,
                last_source_file, last_file_status, failed_jobs, last_error)
               VALUES (1, now(), %s, %s, %s, %s, %s, %s)
               ON CONFLICT (singleton) DO UPDATE SET
                 as_of            = now(),
                 pending_files    = EXCLUDED.pending_files,
                 last_batch_id    = EXCLUDED.last_batch_id,
                 last_source_file = EXCLUDED.last_source_file,
                 last_file_status = EXCLUDED.last_file_status,
                 failed_jobs      = EXCLUDED.failed_jobs,
                 last_error       = EXCLUDED.last_error""",
            (pending,
             row[0] if row else None, row[1] if row else None,
             row[2] if row else None, failed,
             err[0] if err else None))
        cur.execute(
            "INSERT INTO control.runtime_history (as_of, pending_files, "
            "failed_jobs) VALUES (now(), %s, %s)",
            (pending, failed))
        cur.execute("DELETE FROM control.runtime_history "
                    "WHERE as_of < now() - interval '6 hours'")
        cur.connection.commit()
    except Exception:  # noqa: BLE001 - ops tile must never break the pipeline
        log.exception("health update failed")


def stream_daemon(landing=LANDING, interval=5.0):
    """Continuous pkg_etl_control loop: sleep, then process whatever arrived."""
    while True:
        try:
            results, n = process_pending(landing)
            for r in results:
                log.info("batch=%s file=%s clean=%s reject=%s fact=%s alert=%s",
                         r["batch"], r["file"], r["clean"], r["rejected"],
                         r["fact"], r["alerts"])
        except Exception as exc:  # noqa: BLE001 - the daemon must outlive errors
            log.exception("poll failed: %s", exc)
            try:
                c = connect()
                c.close()
            except Exception:  # noqa: BLE001
                pass
        import time
        time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stream_daemon()