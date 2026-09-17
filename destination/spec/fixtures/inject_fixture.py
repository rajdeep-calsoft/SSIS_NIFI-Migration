#!/usr/bin/env python3
"""
Hand one frozen fixture to BOTH engines, identically.

    python3 spec/fixtures/inject_fixture.py tier1-smoke
    python3 spec/fixtures/inject_fixture.py tier1-smoke --only nifi

What it does, and why each step matters:

  1. Rebases every `order_ts` by (now - base_ts). Both engines test order_ts
     against now(), so a fixture left at its stored timestamps would drift past
     the 365-day window and start failing BAD_TIMESTAMP on both sides at once.
     The shift is uniform, so the deliberate `late_timestamp` offsets survive.

  2. Writes ONE rebased file, then copies that same file to both landing
     directories and checksums both. If the two checksums ever differ, the
     comparison downstream is meaningless, so this refuses rather than warns.

  3. Uses one filename per injection, stamped with the run time. The SSIS side
     treats `control.stream_file.source_file` as a UNIQUE watermark, so
     re-injecting under a name it has already seen is an error there.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent

# Where each engine picks files up from. NiFi bind-mounts ./data; the SSIS
# stack bind-mounts its own ./data directory (see its docker-compose.yml), so
# the host can drop a file straight into the engine's inbox.
LANDING = {
    "nifi": ROOT / "data" / "landing",
    "ssis": ROOT.parent / "source" / "data" / "landing",
}


def rebase(path: Path, meta: dict, out: Path) -> tuple[int, int]:
    """Shift every order_ts forward by (now - base_ts). Returns (lines, shift_ms)."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    shift = now_ms - meta["base_ts"]

    lines = 0
    with path.open(encoding="utf-8") as src, out.open("w", encoding="utf-8") as dst:
        for raw in src:
            raw = raw.rstrip("\n")
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except ValueError:
                dst.write(raw + "\n")      # a deliberately unparseable line
                lines += 1
                continue

            ts = record.get("order_ts")
            if isinstance(ts, (int, float)):
                shifted = int(ts) + shift
                record["order_ts"] = shifted
                if "order_ts_iso" in record:
                    record["order_ts_iso"] = datetime.fromtimestamp(
                        shifted / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
            dst.write(json.dumps(record, ensure_ascii=False) + "\n")
            lines += 1
    return lines, shift


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("tier")
    ap.add_argument("--only", choices=sorted(LANDING),
                    help="feed just one engine (default: both)")
    args = ap.parse_args()

    source = HERE / f"{args.tier}.ndjson"
    meta_path = HERE / f"{args.tier}.meta.json"
    if not source.exists():
        print(f"no fixture {args.tier!r}. build it first:\n"
              f"  make fixture TIER={args.tier}", file=sys.stderr)
        return 2
    meta = json.loads(meta_path.read_text())

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    filename = f"{args.tier}_{stamp}.json"

    staged = HERE / f".{filename}.staged"
    lines, shift = rebase(source, meta, staged)
    checksum = digest(staged)

    print(f"{args.tier}: {meta['describe']}")
    print(f"  {meta['orders']:,} orders / {lines:,} lines")
    print(f"  order_ts rebased by {shift / 86_400_000:+.1f} days "
          f"(fixture base {meta['base_ts_iso']})")
    print(f"  sha256 {checksum[:16]}...\n")

    targets = [args.only] if args.only else sorted(LANDING)
    for engine in targets:
        landing = LANDING[engine]
        if not landing.is_dir():
            print(f"  {engine:<5} SKIPPED - no landing dir at {landing}",
                  file=sys.stderr)
            continue
        # Written as .tmp then renamed: NiFi's ListFile watches this directory
        # and must never see a half-written file.
        final = landing / filename
        tmp = final.with_suffix(final.suffix + ".tmp")
        shutil.copyfile(staged, tmp)
        tmp.replace(final)

        got = digest(final)
        if got != checksum:
            print(f"\nREFUSING: {engine} received different bytes\n"
                  f"  expected {checksum}\n  got      {got}", file=sys.stderr)
            return 1
        print(f"  {engine:<5} -> {final}  ✓ identical")

    staged.unlink()
    print(f"\nboth engines hold byte-identical input. Now run each pipeline, "
          f"then:\n  make capture ENGINE=nifi && make capture ENGINE=ssis && make compare")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
