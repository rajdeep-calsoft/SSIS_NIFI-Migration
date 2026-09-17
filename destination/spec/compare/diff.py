#!/usr/bin/env python3
"""
Compare two captured runs and report where the engines disagree.

    python3 spec/compare/diff.py spec/compare/runs/nifi-*.json \
                                 spec/compare/runs/ssis-*.json

With no arguments it picks the newest capture of each engine it can find.

Exit code 0 when the two agree, 1 when they do not -- so this can gate a
migration sign-off.

A difference here is not automatically a bug. Two engines can disagree because
one has a rule the other lacks, because a threshold differs, or because they
were fed different input. The report says WHERE they differ; deciding which
side is right is still a human call.
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
RUNS = HERE / "runs"

GREEN, RED, YELLOW, DIM, BOLD, OFF = (
    "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[1m", "\033[0m")

differences = 0


def flag(msg: str) -> None:
    global differences
    differences += 1
    print(f"  {RED}differs{OFF}  {msg}")


def same(msg: str) -> None:
    print(f"  {GREEN}same{OFF}     {msg}")


def compare_scalars(a: dict, b: dict, na: str, nb: str) -> None:
    print(f"\n{BOLD}Totals{OFF}")
    print(f"  {'':<18} {na:>14} {nb:>14}   {'delta':>10}")
    for key in sorted(set(a["scalars"]) | set(b["scalars"])):
        va, vb = a["scalars"].get(key), b["scalars"].get(key)
        if va is None or vb is None:
            print(f"  {key:<18} {str(va or '-'):>14} {str(vb or '-'):>14}"
                  f"   {YELLOW}not captured{OFF}")
            continue
        delta = vb - va
        mark = f"{GREEN}=={OFF}" if delta == 0 else f"{RED}{delta:+,}{OFF}"
        print(f"  {key:<18} {va:>14,} {vb:>14,}   {mark:>10}")
        if delta:
            global differences
            differences += 1


def compare_histograms(a: dict, b: dict, na: str, nb: str) -> None:
    for name in sorted(set(a["histograms"]) | set(b["histograms"])):
        ha = a["histograms"].get(name, {})
        hb = b["histograms"].get(name, {})
        print(f"\n{BOLD}{name.replace('_', ' ')}{OFF}")
        keys = sorted(set(ha) | set(hb))
        if not keys:
            print(f"  {DIM}(no data on either side){OFF}")
            continue
        print(f"  {'':<20} {na:>12} {nb:>12}")
        for key in keys:
            va, vb = ha.get(key), hb.get(key)
            if va is None:
                print(f"  {key:<20} {'-':>12} {vb:>12,}   "
                      f"{RED}only in {nb}{OFF}")
                differences_inc()
            elif vb is None:
                print(f"  {key:<20} {va:>12,} {'-':>12}   "
                      f"{RED}only in {na}{OFF}")
                differences_inc()
            elif va != vb:
                print(f"  {key:<20} {va:>12,} {vb:>12,}   {RED}{vb - va:+,}{OFF}")
                differences_inc()
            else:
                print(f"  {key:<20} {va:>12,} {vb:>12,}   {GREEN}=={OFF}")


def differences_inc() -> None:
    global differences
    differences += 1


def compare_sets(a: dict, b: dict, na: str, nb: str) -> None:
    for name in sorted(set(a["sets"]) | set(b["sets"])):
        sa, sb = set(a["sets"].get(name, [])), set(b["sets"].get(name, []))
        print(f"\n{BOLD}{name.replace('_', ' ')}{OFF}")
        only_a, only_b, both = sa - sb, sb - sa, sa & sb
        print(f"  in both              {len(both):>12,}")
        if not only_a and not only_b:
            same(f"the two engines produced identical {name.replace('_', ' ')}")
            continue
        if only_a:
            flag(f"{len(only_a):,} only in {na}")
            for value in sorted(only_a)[:5]:
                print(f"           {DIM}{value}{OFF}")
            if len(only_a) > 5:
                print(f"           {DIM}... and {len(only_a) - 5:,} more{OFF}")
        if only_b:
            flag(f"{len(only_b):,} only in {nb}")
            for value in sorted(only_b)[:5]:
                print(f"           {DIM}{value}{OFF}")
            if len(only_b) > 5:
                print(f"           {DIM}... and {len(only_b) - 5:,} more{OFF}")


def newest(engine: str) -> Path | None:
    found = sorted(RUNS.glob(f"{engine}-*.json"))
    return found[-1] if found else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("left", nargs="?", help="first capture (default: newest nifi)")
    ap.add_argument("right", nargs="?", help="second capture (default: newest ssis)")
    args = ap.parse_args()

    left = Path(args.left) if args.left else newest("nifi")
    right = Path(args.right) if args.right else newest("ssis")
    for label, path in (("left", left), ("right", right)):
        if path is None or not path.exists():
            print(f"no {label} capture found — run: "
                  f"python3 spec/compare/capture.py <engine>", file=sys.stderr)
            return 2

    a, b = json.loads(left.read_text()), json.loads(right.read_text())
    na, nb = a["engine"], b["engine"]
    if na == nb:            # comparing two runs of one engine: disambiguate
        na, nb = f"{na}:A", f"{nb}:B"

    print(f"{BOLD}{na}{OFF}  {a['captured_at']}  {DIM}{left.name}{OFF}")
    print(f"{BOLD}{nb}{OFF}  {b['captured_at']}  {DIM}{right.name}{OFF}")

    compare_scalars(a, b, na, nb)
    compare_histograms(a, b, na, nb)
    compare_sets(a, b, na, nb)

    print()
    if differences:
        print(f"{RED}{differences} difference(s){OFF} between {na} and {nb}.")
        print(f"{DIM}A difference is a question, not a verdict — check whether the "
              f"two engines\nsaw the same input and agree on the rules before "
              f"blaming either one.{OFF}")
        return 1
    print(f"{GREEN}no differences{OFF} — {na} and {nb} produced the same result.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
