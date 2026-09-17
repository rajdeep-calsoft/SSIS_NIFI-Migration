#!/usr/bin/env python3
"""
Line this pipeline's rules up against another engine's, rule by rule.

    python3 scripts/rules_diff.py              # nifi vs ssis
    python3 scripts/rules_diff.py --engine ssis

`make compare` answers "did the two engines produce the same rows?". This
answers the question that comes first: "are they even trying to do the same
thing?" -- and it can be answered before the other engine has ever run.

The left column is read out of spec/pipeline.yml, which is generated from the
flow, so it cannot drift from what NiFi actually does. The right column is
read out of spec/engines/<engine>.yml.

Exit code is always 0. A difference here is not a failure -- it is the agenda
for the next conversation with the other team.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
SPEC = ROOT / "spec"

G, R, Y, B, D, OFF = ("\033[32m", "\033[31m", "\033[33m", "\033[1m",
                      "\033[2m", "\033[0m")

# Which of the other engine's rule areas this pipeline can be read for, and
# where to find the answer in pipeline.yml. Kept here rather than in the YAML
# because it is knowledge about how to READ the spec, not part of the spec.
AREAS = [
    ("file_readable",       "can a file be read at all"),
    ("json_parses",         "can a line be parsed"),
    ("required_fields",     "required fields present"),
    ("line_no_parses",      "line_no is an integer"),
    ("types_parse",         "fields are the right type"),
    ("qty_range",           "quantity range"),
    ("price_range",         "unit price range"),
    ("sku_exists",          "sku must exist"),
    ("customer_exists",     "customer must exist"),
    ("timestamp_window",    "order date window"),
    ("currency_whitelist",  "currency whitelist"),
    ("line_total_fix",      "line_total repair"),
    ("dedupe",              "replayed line"),
    ("alert_high_value",    "HIGH_VALUE alert"),
    ("alert_suspicious_qty","SUSPICIOUS_QTY alert"),
]


def nifi_rules(spec: dict) -> dict[str, dict]:
    """Read this pipeline's rules out of the generated contract."""
    stages = {s["id"]: s for s in spec["stages"]}
    rejects = set(spec.get("rejects", {}))
    out: dict[str, dict] = {}

    def add(area, check, outcome=None, reason=None):
        out[area] = {"check": check, "outcome": outcome, "reason": reason}

    guard = stages.get("guard_empty", {})
    add("file_readable", guard.get("condition", "—"), "reject",
        (guard.get("on_fail") or {}).get("becomes"))

    # An unparseable line takes the whole file down, so there is no per-line
    # parse reject to report.
    add("json_parses", "no per-line parse check — the file fails as a whole",
        "reject", "READER_FAILURE" if "READER_FAILURE" in rejects else None)

    # Two stages now, not one: `require_fields` catches an absent field and
    # `validate` catches a present-but-unparseable one. Reading them separately
    # is the whole point -- the other engine reports at that granularity.
    req = stages.get("require_fields", {})
    val = stages.get("validate", {})
    add("required_fields", req.get("condition") or req.get("describe", "—"),
        "reject", (req.get("on_fail") or {}).get("reason"))
    add("types_parse", f"schema validation, strict_types={val.get('strict_types')}",
        "reject", (val.get("on_fail") or {}).get("reason"))
    # line_no is typed `int` in ORDER_LINE_SCHEMA and is half the order_items
    # primary key, so it is caught by the same schema gate as every other type
    # -- there is no separate processor for it. The SSIS side needed an
    # explicit ladder rule to reach the same answer (SSIS-CHANGES.md item 14),
    # so it is reported as its own area on both sides.
    add("line_no_parses", "line_no typed `int` in the reader schema",
        "reject", (val.get("on_fail") or {}).get("reason"))

    # The business rules live as SQL, one query per named reject relationship.
    sql = stages.get("business_rules", {}).get("sql", {})
    reasons = {r["rule"]: r["reason"]
               for r in stages.get("business_rules", {}).get("on_fail", [])}

    def condition(fragment: str) -> str:
        """Pull the readable condition out of the `clean` query."""
        clean = sql.get("clean", "")
        for part in clean.replace("SELECT * FROM FLOWFILE WHERE ", "").split(" AND "):
            if fragment in part:
                return part.strip()
        return "—"

    add("qty_range", condition("qty"), "reject", reasons.get("amounts"))
    add("price_range", condition("unit_price"), "reject", reasons.get("amounts"))
    add("timestamp_window", condition("order_ts"), "reject", reasons.get("timestamp"))
    add("currency_whitelist", condition("currency"), "reject", reasons.get("currency"))

    for stage_id, area in (("enrich_product", "sku_exists"),
                           ("enrich_customer", "customer_exists")):
        st = stages.get(stage_id, {})
        add(area, f"lookup {st.get('lookup','?')} on {st.get('on_key','?')}",
            "reject", (st.get("on_fail") or {}).get("reason"))

    add("line_total_fix", "not done — line_total is loaded as it arrived", None, None)

    dedupe = stages.get("dedupe")
    if dedupe:
        add("dedupe", dedupe.get("describe", "—"), "reject",
            (dedupe.get("on_fail") or {}).get("reason"))
    else:
        keys = (spec.get("targets", {}).get("order_items", {}) or {}).get("keys", [])
        add("dedupe", f"absorbed silently by the UPSERT key ({', '.join(keys)})",
            "upsert", None)

    for alert in spec.get("alerts", []):
        area = f"alert_{alert['name'].lower()}"
        out[area] = {"check": f"{alert['condition']}  (per {alert.get('scope')})",
                     "outcome": "alert", "reason": alert["name"]}
    return out


def other_rules(binding: dict) -> dict[str, dict]:
    """Index the other engine's binding by area, and refuse to skip any of it.

    AREAS above is a hand-written list, so a rule added to ssis.yml under a new
    area used to be dropped without a word: the summary line simply counted one
    rule fewer than the file held. That is exactly the failure `make spec`
    refuses on for processors, and it hid the `line_no_parses` rule on the run
    that added it. Same rule here -- an area the differ cannot read is a bug in
    the differ, not something to pass over quietly.
    """
    rules = {r["area"]: r for r in binding.get("rules", [])}
    unknown = sorted(set(rules) - {a for a, _ in AREAS})
    if unknown:
        raise SystemExit(
            f"{R}rules-diff refused: spec/engines/ssis.yml has rule area(s) the "
            f"differ cannot read{OFF}\n"
            + "".join(f"  - {a}\n" for a in unknown)
            + "add each to AREAS in scripts/rules_diff.py, with a matching "
              "add(...) in nifi_rules() so both engines are read for it.")
    return rules


def wrap(text: str, width: int) -> list[str]:
    words, lines, cur = str(text).split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > width:
            lines.append(cur); cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    return lines or [""]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", default="ssis", help="engine to compare against")
    args = ap.parse_args()

    spec = yaml.safe_load((SPEC / "pipeline.yml").read_text())
    path = SPEC / "engines" / f"{args.engine}.yml"
    if not path.exists():
        print(f"no binding at {path.relative_to(ROOT)}", file=sys.stderr)
        return 2
    binding = yaml.safe_load(path.read_text())

    mine, theirs = nifi_rules(spec), other_rules(binding)
    name = args.engine

    print(f"\n{B}rule-by-rule: nifi vs {name}{OFF}")
    print(f"{D}left  — spec/pipeline.yml (generated from the flow){OFF}")
    print(f"{D}right — spec/engines/{name}.yml "
          f"(from {binding.get('source_of_truth','?')}){OFF}\n")

    same = differs = only_theirs = 0
    W = 44
    for area, label in AREAS:
        a, b = mine.get(area), theirs.get(area)
        if b is None:
            continue
        ra = (a or {}).get("reason")
        rb = b.get("reason")
        # Reason and outcome are compared mechanically. Whether the CONDITION
        # also matches is a human reading, declared per rule as `status`:
        #
        #   agreed  — this pipeline implements his rule; any note is history
        #   differs — the wording really does differ, and the note says how
        #   open    — a known, measured divergence nobody has settled yet
        #
        # A bare `note:` with no status still means "differs": that was the
        # original convention and several rules predate the field.
        reason_same = bool(a) and ra == rb and a.get("outcome") == b.get("outcome")
        status = b.get("status") or ("differs" if b.get("note") else "agreed")
        agreed = reason_same and status == "agreed"
        if agreed:
            same += 1
            mark = f"{G}same{OFF}"
        elif a is None or (a.get("outcome") is None and b.get("outcome")):
            only_theirs += 1
            mark = f"{R}missing here{OFF}"
        elif reason_same:
            differs += 1
            mark = f"{Y}same reason · rule differs{OFF}"
        else:
            differs += 1
            mark = f"{R}differs{OFF}"

        flag = f" {R}[contested]{OFF}" if b.get("contested") else ""
        print(f"{B}{label}{OFF}  {mark}{flag}")
        la = wrap((a or {}).get("check", "not implemented"), W)
        lb = wrap(b.get("check", ""), W)
        for i in range(max(len(la), len(lb))):
            print(f"   {la[i] if i < len(la) else '':<{W}}  │  "
                  f"{lb[i] if i < len(lb) else ''}")
        pa = f"{(a or {}).get('outcome') or '—'} {ra or ''}".strip()
        pb = f"{b.get('outcome') or '—'} {rb or ''}".strip()
        colour_a = G if agreed else Y
        print(f"   {colour_a}{pa:<{W}}{OFF}  │  {colour_a}{pb}{OFF}")
        if b.get("note"):
            for line in wrap(b["note"], 92):
                print(f"   {D}{line}{OFF}")
        print()

    # reject vocabulary, compared literally -- these strings are the contract
    ra, rb = set(spec.get("rejects", {})), set(binding.get("rejects", {}))
    print(f"{B}reject vocabulary{OFF}")
    print(f"   {'in both':<22}{G}{', '.join(sorted(ra & rb)) or '—'}{OFF}")
    print(f"   {'only nifi':<22}{Y}{', '.join(sorted(ra - rb)) or '—'}{OFF}")
    print(f"   {f'only {name}':<22}{R}{', '.join(sorted(rb - ra)) or '—'}{OFF}")

    print(f"\n{B}summary{OFF}  {G}{same} agree{OFF}  ·  {Y}{differs} differ{OFF}"
          f"  ·  {R}{only_theirs} missing here{OFF}")
    print(f"{D}A difference is the agenda for the next conversation, not a bug.\n"
          f"Rules marked [contested] depend on the input data, so settle the\n"
          f"dataset before settling the number.{OFF}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
