#!/usr/bin/env python3
"""
Read one engine's warehouse into a neutral run summary.

    python3 spec/compare/capture.py nifi
    python3 spec/compare/capture.py ssis --label after-fix

Writes spec/compare/runs/<engine>-<timestamp>.json. Feed two of those to
diff.py to compare the engines.

Why a capture step at all: the two engines cannot be up at the same time --
both bind host port 5433 -- so they can never be queried in one pass. Each is
captured while it is the one running, and the comparison happens later on the
two files. That also means a capture is a durable record: you can re-compare
last week's NiFi run against today's SSIS run without re-running either.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
ADAPTERS = HERE / "adapters.yml"
RUNS = HERE / "runs"
SEP = "\x1f"


def psql(adapter: dict, query: str) -> list[list[str]]:
    compose = (ROOT / adapter["compose_file"]).resolve()
    # ASCII unit separator, not "|": the reject_keys values contain pipes, and
    # a colliding field separator silently truncates every one of them.
    cmd = (["docker", "compose", "-f", str(compose), "exec", "-T",
            adapter["service"]] + adapter["psql"] + ["-tAF", SEP, "-c", query])
    out = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if out.returncode != 0:
        raise RuntimeError((out.stderr.strip() or "psql failed").splitlines()[-1])
    return [line.split(SEP) for line in out.stdout.strip().splitlines() if line]


def capture(engine: str, adapter: dict) -> dict:
    scalars, histograms, sets = {}, {}, {}

    for name, query in adapter.get("scalars", {}).items():
        rows = psql(adapter, query)
        scalars[name] = int(rows[0][0]) if rows and rows[0][0] else 0
        print(f"  {name:<18} {scalars[name]:>10,}")

    for name, query in adapter.get("histograms", {}).items():
        rows = psql(adapter, query)
        histograms[name] = {r[0]: int(r[1]) for r in rows if len(r) >= 2}
        print(f"  {name:<18} {len(histograms[name])} distinct")

    for name, query in adapter.get("sets", {}).items():
        rows = psql(adapter, query)
        # sorted so two captures of the same state produce identical files
        sets[name] = sorted({r[0] for r in rows})
        print(f"  {name:<18} {len(sets[name]):>10,} values")

    return {
        "engine": engine,
        "describe": adapter.get("describe", ""),
        "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "scalars": scalars,
        "histograms": histograms,
        "sets": sets,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("engine", help="which adapter to use (see adapters.yml)")
    ap.add_argument("--label", help="suffix for the output filename")
    args = ap.parse_args()

    adapters = yaml.safe_load(ADAPTERS.read_text())
    if args.engine not in adapters:
        print(f"unknown engine {args.engine!r}. known: {', '.join(adapters)}",
              file=sys.stderr)
        return 2

    print(f"capturing '{args.engine}' — {adapters[args.engine].get('describe', '')}\n")
    try:
        summary = capture(args.engine, adapters[args.engine])
    except RuntimeError as exc:
        print(f"\ncannot query the {args.engine} warehouse: {exc}", file=sys.stderr)
        print("is that engine's stack up? only one can hold port 5433 at a time.",
              file=sys.stderr)
        return 1

    RUNS.mkdir(exist_ok=True)
    stamp = summary["captured_at"].replace(":", "").replace("-", "")
    name = f"{args.engine}-{stamp}" + (f"-{args.label}" if args.label else "")
    path = RUNS / f"{name}.json"
    path.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\nwrote {path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
