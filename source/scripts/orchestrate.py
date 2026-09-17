#!/usr/bin/env python3
"""
End-to-end orchestration for the SSIS streaming ETL.

Steps:
  1. (re)build schema + seeds on Postgres (incl. streaming source tables)
  2. build the 6 SSIS .dtsx packages (streaming-landing specs)
  3. generate a burst of streaming NDJSON batches via orders_stream (MAX_BATCHES)
  4. run the SSIS simulator in STREAM mode: pkg_ingest_stage -> ... -> pkg_reconcile
  5. write stream_summary.json report + verify every batch reconciled PASS
  6. run the streaming test suite

For a live, continuous demo instead: `docker compose up -d orders_gen` streams
forever and `python -m ssis_sim.stream_runner stream_daemon` consumes it.

Usage:  python scripts/orchestrate.py [--skip-gen] [--gen-batches N]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "ssis_sim"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

DB = {
    "host": os.environ.get("PG_HOST", "postgres"),
    "port": int(os.environ.get("PG_PORT", 5432)),
    "user": os.environ.get("PG_USER", "etl_user"),
    "password": os.environ.get("PG_PASS", "etl_pass"),
    "database": os.environ.get("PG_DB", "etl_db"),
}
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(ROOT, "data"))
LANDING = os.environ.get("LANDING_DIR", os.path.join(DATA_DIR, "landing"))
GEN_DIR = os.path.join(ROOT, "orders_stream")


def psql(script):
    import psycopg2
    from apply_sql import apply_sql_path
    c = psycopg2.connect(connect_timeout=15, **DB)
    failed = apply_sql_path(c, script)
    c.close()
    for f, e in failed:
        print(f"  [warn] {f!r}: {e}")


def init_schema():
    # shared_dims.sql must run after stream_schema.sql creates the tables:
    # it is the catalogue shared with the NiFi engine (see its header).
    for f in ("init_schema.sql", "scd2_customer.sql", "seed_dims.sql",
              "stream_schema.sql", "shared_dims.sql"):
        psql(os.path.join(ROOT, "scripts", f))


def build_packages():
    from ssis_sim.dtsx_builder import build_all
    pkg_dir = os.path.join(ROOT, "ssis_packages")
    build_all(pkg_dir)
    return pkg_dir


def clear_landing():
    os.makedirs(LANDING, exist_ok=True)
    for name in os.listdir(LANDING):
        if name.endswith(".json"):
            os.remove(os.path.join(LANDING, name))
    print(f"  [gen] cleared landing dir {LANDING}")


def gen_burst(batches, interval, size, seed):
    os.makedirs(LANDING, exist_ok=True)
    env = dict(os.environ)
    env.update({
        "PGHOST": DB["host"], "PGPORT": str(DB["port"]),
        "PGDATABASE": DB["database"], "PGUSER": DB["user"],
        "PGPASSWORD": DB["password"],
        "LANDING_DIR": LANDING,
        "BATCH_INTERVAL": str(interval),
        "BATCH_SIZE": str(size),
        "ERROR_RATE": "0.06",
        "GEN_SEED": str(seed),
        "MAX_BATCHES": str(batches),
    })
    subprocess.run([sys.executable, "-m", "gen", "stream"], cwd=GEN_DIR,
                   env=env, check=True)


def run_stream():
    from ssis_sim import stream_runner as R
    results, n = R.process_pending(LANDING)
    for r in results:
        ok = all(c["pass"] for c in r["checks"])
        print(f"  [batch {r['batch']}] {r['file']} clean={r['clean']} "
              f"rejected={r['rejected']} fact={r['fact']} alerts={r['alerts']} "
              f"{'PASS' if ok else 'FAIL'}")
    return results, n


def write_report(results, n):
    import psycopg2
    from ssis_sim import stream_runner as R

    status = R.stream_status()
    checks_ok = all(c["pass"] for r in results for c in r["checks"])

    c = psycopg2.connect(**DB)
    cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM control.reconcile_results WHERE NOT pass")
    failed_recs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM control.stream_file WHERE status='FAIL'")
    failed_files = cur.fetchone()[0]
    cur.close()
    c.close()

    payload = {
        "mode": "stream",
        "batches_this_run": len(results),
        "files_pending_processed": n,
        "totals": status,
        "reconcile_fails": failed_recs,
        "files_failed": failed_files,
        "verdict": "PASS" if (checks_ok and failed_recs == 0
                              and failed_files == 0) else "FAIL",
    }
    d = os.path.join(DATA_DIR, "reports")
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, "stream_summary.json")
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-gen", action="store_true")
    ap.add_argument("--gen-batches", type=int, default=8)
    ap.add_argument("--gen-interval", type=float, default=1.0)
    ap.add_argument("--gen-size", type=int, default=50)
    ap.add_argument("--gen-seed", type=int, default=42)
    ap.add_argument("--skip-tests", action="store_true")
    ap.add_argument("--fresh", action="store_true",
                    help="clear processed-file ledger + result tables + landing")
    ap.add_argument("--daemon", action="store_true",
                    help="loop forever consuming new batches")
    args = ap.parse_args()

    init_schema()
    print("[1/6] schema + streams seeds ready")

    pkg_dir = build_packages()
    print(f"[2/6] built {len(os.listdir(pkg_dir))} .dtsx packages")

    if args.fresh:
        clear_landing()
        import psycopg2
        for t in ("stage.sales_stage", "stage.clean_sales",
                  "control.dlq_errors", "control.reconcile_results",
                  "control.data_quality_metrics", "control.stream_file",
                  "control.stream_alerts", "control.etl_run_log",
                  "control.etl_batch", "control.pipeline_runtime", "control.runtime_history",
                  "dw.fact_sales", "dw.dim_customer",
                  "dw.dim_product"):
            c = psycopg2.connect(**DB)
            cur = c.cursor()
            cur.execute(f"TRUNCATE {t} RESTART IDENTITY CASCADE")
            c.commit()
            cur.close()
            c.close()
        print("[3/6] fresh start: tables reset")

    if not args.skip_gen:
        gen_burst(args.gen_batches, args.gen_interval, args.gen_size,
                  args.gen_seed)
        print(f"[3/6] generated {args.gen_batches} streaming batches -> {LANDING}")
    else:
        print("[3/6] skipped generator (using whatever is in landing)")

    if args.daemon:
        from ssis_sim import stream_runner as R
        print("[4/6] streaming daemon starting (Ctrl-C to stop)")
        R.stream_daemon(LANDING, interval=5.0)
        return

    results, n = run_stream()
    print(f"[4/6] processed {len(results)} new batches ({n} files)")

    path, payload = write_report(results, n)
    print(f"[5/6] report -> {path}")
    print(f"      totals: staged={payload['totals']['staged']} "
          f"clean-in-fact={payload['totals']['fact']} "
          f"rejected={payload['totals']['rejected']} "
          f"customers={payload['totals']['customers_current']} "
          f"products={payload['totals']['products']}")

    if not args.skip_tests:
        test_env = {**os.environ, "PG_DB": "etl_test",
                    "PG_HOST": os.environ.get("PG_HOST", "postgres")}
        rc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q",
             os.path.join(ROOT, "tests", "test_stream.py"),
             "-p", "no:cacheprovider"],
            cwd=ROOT, env=test_env).returncode
        if rc != 0:
            sys.exit(rc)

    print("\n=== STREAMING E2E RESULT:", payload["verdict"], "===")
    sys.exit(0 if payload["verdict"] == "PASS" else 1)


if __name__ == "__main__":
    main()