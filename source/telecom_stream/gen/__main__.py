"""CLI: python -m gen bulk --rows 50000 [--batch-size 500] [--error-rate 0.08]

Generates the requested number of synthetic telecom CDRs, split into
batch-size chunks (multiple landing files, not one huge one -- mirrors the
reference repo's own orders_stream generator, which the exploration behind
this repo's design found already avoids single giant landing files for the
same reason: NiFi processes a file as a unit, and a single 50,000-row file
means zero visibility into partial progress). Every batch's ground truth is
loaded into source Postgres as it's written (see emit.write_batch).
"""
from __future__ import annotations

import argparse
import os
import pathlib
import random
import sys
import time

import psycopg2

from . import emit, model


def _connect():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "telecom"),
        user=os.environ.get("PGUSER", "telecom_etl"),
        password=os.environ["PGPASSWORD"],
    )


def cmd_bulk(args: argparse.Namespace) -> int:
    conn = _connect()
    catalog = model.Catalog.load(conn)
    rng = random.Random(args.seed)
    landing_dir = pathlib.Path(args.landing_dir)

    total_loaded = total_rejected = 0
    reason_totals: dict[str, int] = {}
    seq = 0
    remaining = args.rows
    t0 = time.time()
    while remaining > 0:
        n = min(args.batch_size, remaining)
        summary = emit.write_batch(conn, catalog, rng, seq, n, args.error_rate, landing_dir)
        seq += n
        remaining -= n
        total_loaded += summary["loaded"]
        total_rejected += summary["rejected"]
        for reason, count in summary["reasons"].items():
            reason_totals[reason] = reason_totals.get(reason, 0) + count
        print(f"  {summary['source_file']}: {summary['rows']} rows "
              f"({summary['loaded']} clean, {summary['rejected']} rejected)", file=sys.stderr)

    elapsed = time.time() - t0
    print(f"\nwrote {args.rows} rows in {seq // args.batch_size + (1 if seq % args.batch_size else 0)} "
          f"file(s), {elapsed:.1f}s", file=sys.stderr)
    print(f"  clean:    {total_loaded}", file=sys.stderr)
    print(f"  rejected: {total_rejected}", file=sys.stderr)
    for reason, count in sorted(reason_totals.items()):
        print(f"    {reason}: {count}", file=sys.stderr)
    conn.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="gen")
    sub = ap.add_subparsers(dest="cmd", required=True)

    bulk = sub.add_parser("bulk", help="generate N synthetic CDRs in one run")
    bulk.add_argument("--rows", type=int, default=50000)
    bulk.add_argument("--batch-size", type=int, default=500)
    bulk.add_argument("--error-rate", type=float,
                       default=float(os.environ.get("ERROR_RATE", "0.08")))
    bulk.add_argument("--seed", type=int, default=int(os.environ.get("GEN_SEED", "42")))
    bulk.add_argument("--landing-dir", default=os.environ.get("LANDING_DIR", "/data/landing"))
    bulk.set_defaults(func=cmd_bulk)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
