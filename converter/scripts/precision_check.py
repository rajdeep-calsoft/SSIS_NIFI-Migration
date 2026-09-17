#!/usr/bin/env python3
"""The global precision checker.

Runs every real .dtsx file in the repo through the analyzer and reports one
number: what fraction of components in genuine packages this converter can
faithfully translate. This is the stop condition for iterating on the
catalogue -- exit 0 means "every genuine package in the repo converts at
100%, nothing left to add"; exit 1 means "here is exactly what's missing,
ranked by how many packages it blocks."

Deliberately excludes files the analyzer correctly refuses as NOT a genuine
.dtsx (malformed XML, fake namespace) -- those aren't conversion targets,
they're negative tests, and counting them against precision would make
"refuse a non-package correctly" look like a failure.

    python3 scripts/precision_check.py
    python3 scripts/precision_check.py --root ../source/ssis_packages
"""
from __future__ import annotations

import argparse
import pathlib
import sys
from collections import Counter

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from ssis2nifi.catalog.derive import derive
from ssis2nifi.catalog.support import annotate, classify
from ssis2nifi.dtsx.parse import NotADtsxPackage, parse_file

HERE = pathlib.Path(__file__).resolve().parent
DEFAULT_ROOTS = [HERE.parent / "corpus" / "packages", HERE.parent.parent / "source" / "ssis_packages"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", action="append", type=pathlib.Path,
                     help="directory to scan for .dtsx (repeatable); default: corpus + source packages")
    args = ap.parse_args()
    roots = args.root or DEFAULT_ROOTS

    files: list[pathlib.Path] = []
    for root in roots:
        files.extend(sorted(root.glob("*.dtsx")))

    not_a_package: list[tuple[str, str]] = []
    genuine: list[tuple[str, int, int, list[str]]] = []
    addable_counter: Counter[str] = Counter()   # verdict == unknown: no rule yet, could add one
    permanent_counter: Counter[str] = Counter()  # verdict == refused: unsafe to ever auto-convert

    for f in files:
        try:
            pkg = derive(annotate(parse_file(str(f))))
        except NotADtsxPackage as exc:
            not_a_package.append((f.name, str(exc)))
            continue

        reasons = []
        for df in pkg.dataflows:
            for comp in df.components:
                verdict, reason = classify(comp)
                if verdict == "supported":
                    continue
                label = comp.class_id or comp.raw_class_id
                reasons.append(f"{label}: {reason}")
                (permanent_counter if verdict == "refused" else addable_counter)[label] += 1
        genuine.append((f.name, pkg.coverage.recognised, pkg.coverage.total_components, reasons))

    print(f"{'PACKAGE':<38} {'SCORE':<10} STATUS")
    total_recognised = total_components = 0
    for name, recognised, total, reasons in genuine:
        total_recognised += recognised
        total_components += total
        pct = 100.0 * recognised / total if total else 100.0
        status = "100%" if not reasons else f"blocked by: {reasons[0]}" + (f" (+{len(reasons)-1} more)" if len(reasons) > 1 else "")
        print(f"{name:<38} {recognised}/{total:<7} {pct:5.0f}%  {status}")

    for name, reason in not_a_package:
        print(f"{name:<38} {'n/a':<10} not a genuine .dtsx ({reason[:60]})")

    overall = 100.0 * total_recognised / total_components if total_components else 100.0
    print()
    print(f"OVERALL PRECISION: {total_recognised}/{total_components} components "
          f"across {len(genuine)} genuine package(s) = {overall:.1f}%")
    print(f"(+{len(not_a_package)} file(s) correctly identified as not genuine .dtsx, excluded from the score)")

    if addable_counter:
        print("\nADDABLE GAPS (no conversion rule yet -- write a catalogue recipe to close these):")
        for label, count in addable_counter.most_common():
            print(f"  {count:>2}x  {label}")

    if permanent_counter:
        print("\nPERMANENT REFUSALS (unsafe to ever auto-convert -- refusing IS the correct, done state):")
        for label, count in permanent_counter.most_common():
            print(f"  {count:>2}x  {label}")

    if not addable_counter:
        print("\nDONE: no addable gaps remain. Every unsupported component left is a deliberate, "
              "permanent refusal (arbitrary code or a semantics mismatch NiFi cannot faithfully express).")
        return 0
    print(f"\nNOT DONE: {sum(addable_counter.values())} component instance(s) have no conversion rule yet. "
          "Close the top ADDABLE gap and re-run.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
