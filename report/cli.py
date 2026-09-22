#!/usr/bin/env python3
"""report -- generates a standalone SSIS-vs-NiFi comparison report for a job.

    python3 report/cli.py compare-report --job telecom_cdr

Connects directly to both Postgres instances job.yml names (source_db,
destination_db -- published ports, no FDW, no shared Docker network), reads
NiFi's own state over its REST API, and writes a self-contained HTML +
JSON report under out/reports/. Nothing here is specific to telecom_cdr:
point --job at a different job.yml with its own `compare:` block and the
exact same code runs.
"""
from __future__ import annotations

import argparse
import datetime
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "migrator"))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import jobconfig  # noqa: E402

from report import capture, diff, introspect, pipeline_state, render  # noqa: E402


def cmd_compare_report(args: argparse.Namespace) -> int:
    job = jobconfig.resolve(args.job)
    compare_cfg = job.compare
    if not compare_cfg:
        print(f"{job.path}: no `compare:` block -- nothing to report", file=sys.stderr)
        return 2

    print(f"connecting to source ({job.source_db['host']}:{job.source_db['port']}) "
          f"and destination ({job.destination_db['host']}:{job.destination_db['port']})...",
          file=sys.stderr)
    source_conn = introspect.connect(job.source_db)
    dest_conn = introspect.connect(job.destination_db)

    print("capturing source...", file=sys.stderr)
    source_capture = capture.capture(source_conn, compare_cfg, "SSIS (source)")
    print("capturing destination...", file=sys.stderr)
    dest_capture = capture.capture(dest_conn, compare_cfg, "NiFi (destination)")

    result = diff.compare(source_capture, dest_capture)

    nifi_state = {"found": False, "group_name": job.name}
    if job.raw.get("nifi", {}).get("url"):
        print("reading NiFi pipeline state...", file=sys.stderr)
        nifi_state = pipeline_state.nifi_group_state(job.nifi_url, job.name)

    source_runs = []
    if job.ledger:
        source_runs = pipeline_state.source_job_runs(source_conn, job.ledger["table"])

    source_conn.close()
    dest_conn.close()

    out_dir = pathlib.Path(args.out or "out/reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    html_path = out_dir / f"{job.name}-{stamp}.html"
    json_path = out_dir / f"{job.name}-{stamp}.json"

    html_path.write_text(render.render_html(job.name, result, nifi_state, source_runs))
    json_path.write_text(render.to_json(job.name, result, nifi_state, source_runs))

    print(f"\nwrote {html_path}")
    print(f"wrote {json_path}")
    print(f"\nverdict: {'AGREES' if result.agrees else 'DIFFERENCES FOUND'}")
    for s in result.scalars:
        mark = "==" if s.agrees else f"{(s.destination or 0) - (s.source or 0):+d}"
        print(f"  {s.key:<16} source={s.source!s:>8} destination={s.destination!s:>8}   {mark}")

    return 0 if result.agrees else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="report", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    cr = sub.add_parser("compare-report", help="generate a standalone SSIS-vs-NiFi report")
    cr.add_argument("--job", required=True, help="job.yml path or a bare name under jobs/")
    cr.add_argument("-o", "--out", help="directory to write the report into (default: out/reports)")
    cr.set_defaults(func=cmd_compare_report)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
