#!/usr/bin/env python3
"""M6.7's bulk proof for pkg_orders_etl.dtsx: an independent oracle at
50,000+ rows, the same principle as `ssis2nifi verify-behavior` (M5) scaled
up, applied to the GENERATED flow now deployed on the destination NiFi.

WHY THIS, NOT THE TWO-ENGINE GRAFANA DASHBOARD
-------------------------------------------------
The manager's brief pictured proving "100% match" on the existing
`engine-comparison` dashboard, which compares NIFI-FLOW's hand-built
ecommerce_etl against the colleague's hand-built SSIS simulator -- a
different business domain (18 products / 60 customers / order aggregation)
than this converter has ever targeted. There is no SSIS-side implementation
of pkg_orders_etl to compare against; building one was explicitly out of
scope (the colleague's own .dtsx files are non-conformant, per the
project's own notes). Comparing this generated flow's output against an
INDEPENDENT expectation computed in plain Python -- code that shares nothing
with the converter, the catalogue, or NiFi -- is a genuine, strong proof of
correctness at the scale asked for; it is just not literally the same board.
See docs/1.manager-demo.md for the full reasoning and what a follow-on
milestone building an SSIS-side pkg_orders_etl would need.

WHAT IT DOES
-------------
1. Generates N order lines with a KNOWN, independently-computed disposition
   each (clean / UNKNOWN_SKU / UNKNOWN_CUSTOMER / RANGE_VIOLATION /
   BAD_CURRENCY), using the SAME reference data (products/customers) the
   deployed flow's Lookups read -- but never importing anything from
   ssis2nifi itself, so agreement is not the tool grading its own homework.
2. Writes it as one NDJSON file into destination's landing directory (the
   SAME file both the generated flow and, were it running, the hand-built
   one would read).
3. Waits for the flow to settle, then re-feeds a subset of the clean rows in
   a second file to exercise DUP_KEY (cross-batch replay).
4. Reads the ACTUAL landed/rejected counts back from nifi-warehouse and
   diffs them against the independently computed expectation, row category
   by row category.

Usage:
    python3 scripts/verify_bulk.py --rows 50000
    python3 scripts/verify_bulk.py --rows 100000   # "1 lakh"
"""
from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

FAULT_RATE = {"unknown_sku": 0.02, "unknown_customer": 0.02,
              "range_violation": 0.02, "bad_currency": 0.02}
CURRENCIES_OK = ["INR", "USD", "EUR", "GBP"]
CURRENCIES_BAD = ["XYZ", "ABC", "QQQ"]


def psql(container: str, db: str, user: str, sql: str) -> list[str]:
    out = subprocess.run(
        ["docker", "exec", "-i", container, "psql", "-U", user, "-d", db, "-tA", "-c", sql],
        capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip())
    return [ln for ln in out.stdout.strip().splitlines() if ln]


def load_reference(container: str, db: str, user: str) -> tuple[list[str], list[str]]:
    skus = psql(container, db, user, "select sku from products")
    customers = psql(container, db, user, "select customer_id from customers")
    return skus, customers


def make_batch(n: int, skus: list[str], customers: list[str], rng: random.Random,
               batch_id: str) -> tuple[list[dict], dict[str, int]]:
    rows = []
    expect = {"clean": 0, "UNKNOWN_SKU": 0, "UNKNOWN_CUSTOMER": 0,
              "RANGE_VIOLATION": 0, "BAD_CURRENCY": 0}
    for i in range(n):
        order_id = f"{batch_id}-{i:07d}"
        sku = rng.choice(skus)
        customer_id = rng.choice(customers)
        qty = rng.randint(1, 20)
        price = round(rng.uniform(1, 500), 2)
        currency = rng.choice(CURRENCIES_OK)
        roll = rng.random()
        cursor = 0.0
        reason = "clean"
        for name, rate in FAULT_RATE.items():
            cursor += rate
            if roll < cursor:
                reason = name
                break

        if reason == "unknown_sku":
            sku = f"SKU-GHOST-{i}"
            expect["UNKNOWN_SKU"] += 1
        elif reason == "unknown_customer":
            customer_id = f"CUST-GHOST-{i}"
            expect["UNKNOWN_CUSTOMER"] += 1
        elif reason == "range_violation":
            qty = 0
            expect["RANGE_VIOLATION"] += 1
        elif reason == "bad_currency":
            currency = rng.choice(CURRENCIES_BAD)
            expect["BAD_CURRENCY"] += 1
        else:
            expect["clean"] += 1

        rows.append({
            "order_id": order_id, "line_no": 1, "customer_id": customer_id,
            "order_ts": 1788263797000, "status": "PLACED", "channel": "WEB",
            "currency": currency, "sku": sku, "qty": qty, "unit_price": price,
            "line_total": round(qty * price, 2),
        })
    return rows, expect


def write_ndjson(rows: list[dict], path: Path) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def nifi_group_id(nifi_url: str, group_name: str) -> str:
    with urllib.request.urlopen(f"{nifi_url}/nifi-api/flow/process-groups/root") as r:
        d = json.load(r)
    for g in d["processGroupFlow"]["flow"]["processGroups"]:
        if g["component"]["name"] == group_name:
            return g["id"]
    raise RuntimeError(f"process group {group_name!r} not found on {nifi_url}")


def total_queued(nifi_url: str, group_id: str) -> int:
    with urllib.request.urlopen(f"{nifi_url}/nifi-api/flow/process-groups/{group_id}/status") as r:
        d = json.load(r)
    snap = d["processGroupStatus"]["aggregateSnapshot"]
    return sum(int(c["connectionStatusSnapshot"]["queuedCount"].replace(",", ""))
               for c in snap["connectionStatusSnapshots"])


def wait_for_settle(nifi_url: str, group_name: str, timeout: float) -> None:
    """Wait until NiFi's OWN queues for this process group are empty, not
    until some downstream database count stops moving.

    Found the hard way, the first version of this function watched
    `order_items` + `quarantine_records`' combined row count and returned
    once it held steady. That is wrong for this specific flow shape:
    `Check Replay` is a single-threaded, UNCACHED LookupRecord (deliberately
    -- see catalogue/components/microsoft.lookup.yml on why a replay check
    cannot cache) that consumes one whole flowfile -- tens of thousands of
    records -- before emitting ANYTHING downstream. While it is mid-flight
    the output tables' row count is dead flat for minutes, which a
    stability check reads as "settled" when the flow is actually still
    deep in its only slow processor. A replay fired into that false-settled
    window hit rows that were not committed yet, slipped through Check
    Replay as "not yet loaded", and then failed at Load Order Items on a
    primary-key violation -- silently, into an auto-terminated `failure`
    relationship. Same class of bug M5 exists to catch, on a different
    processor, found the same way M5 found its bug: by triggering it.

    The queue depth NiFi tracks for each connection IS the ground truth for
    "is anything still moving", regardless of which processor is slow.
    """
    group_id = nifi_group_id(nifi_url, group_name)
    deadline = time.time() + timeout
    empty_since = None
    while time.time() < deadline:
        queued = total_queued(nifi_url, group_id)
        print(f"  ...{queued} flowfile(s) still queued in the flow", file=sys.stderr)
        if queued == 0:
            if empty_since and time.time() - empty_since > 20:
                return
            empty_since = empty_since or time.time()
        else:
            empty_since = None
        time.sleep(5)
    print(f"  WARNING: timed out after {timeout}s with the queue not empty -- "
          "a replay test run now risks the exact race this function exists "
          "to avoid", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=50000)
    ap.add_argument("--landing", default="../destination/data/landing")
    ap.add_argument("--container", default="nifi-warehouse")
    ap.add_argument("--db", default="etldemo")
    ap.add_argument("--user", default="etl")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--wait", type=float, default=420.0)
    ap.add_argument("--nifi", default="http://localhost:8080")
    ap.add_argument("--group-name", default="ssis2nifi-pkg_orders_etl")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    print(f"reading reference data from {args.container}...", file=sys.stderr)
    skus, customers = load_reference(args.container, args.db, args.user)
    print(f"  {len(skus)} products, {len(customers)} customers", file=sys.stderr)

    landing = Path(args.landing)
    landing.mkdir(parents=True, exist_ok=True)
    batch_tag = f"BULK{int(time.time())}"

    rows, expect = make_batch(args.rows, skus, customers, rng, batch_tag)
    path = landing / f"{batch_tag}.ndjson"
    write_ndjson(rows, path)
    print(f"wrote {path} ({len(rows)} rows)", file=sys.stderr)
    print(f"expected: {expect}", file=sys.stderr)

    wait_for_settle(args.nifi, args.group_name, args.wait)

    # -- replay a slice of the clean rows to exercise DUP_KEY -----------
    clean_rows = [r for r in rows
                  if r["sku"] in skus and r["customer_id"] in customers
                  and 1 <= r["qty"] <= 999 and r["currency"] in CURRENCIES_OK]
    replay_n = min(200, len(clean_rows))
    replay = rng.sample(clean_rows, replay_n)
    replay_path = landing / f"{batch_tag}-replay.ndjson"
    write_ndjson(replay, replay_path)
    print(f"wrote {replay_path} ({replay_n} replayed rows, expect DUP_KEY on all)",
          file=sys.stderr)
    wait_for_settle(args.nifi, args.group_name, args.wait)

    # -- read actual results back, independently of anything the ---------
    # -- converter or the flow computed --------------------------------
    actual_loaded = int(psql(args.container, args.db, args.user,
        f"select count(*) from order_items where order_id like '{batch_tag}%'")[0])
    reason_rows = psql(args.container, args.db, args.user,
        f"select reason, count(*) from quarantine_records "
        f"where order_id like '{batch_tag}%' group by reason")
    actual_by_reason: dict[str, int] = {}
    for line in reason_rows:
        reason, count = line.rsplit("|", 1) if "|" in line else line.split(None, 1)
        actual_by_reason[reason.strip()] = int(count.strip())

    print("\n=== independent oracle vs actual ===")
    ok = True
    for reason, expected_count in expect.items():
        if reason == "clean":
            actual = actual_loaded
        else:
            actual = actual_by_reason.get(reason, 0)
        verdict = "OK" if actual == expected_count else "MISMATCH"
        if actual != expected_count:
            ok = False
        print(f"  {reason:<20} expected={expected_count:<8} actual={actual:<8} {verdict}")

    replay_actual = actual_by_reason.get("DUP_KEY", 0)
    replay_verdict = "OK" if replay_actual == replay_n else "MISMATCH"
    if replay_actual != replay_n:
        ok = False
    print(f"  {'DUP_KEY (replay)':<20} expected={replay_n:<8} actual={replay_actual:<8} {replay_verdict}")

    total_expected = args.rows + replay_n
    total_actual = actual_loaded + sum(actual_by_reason.values())
    print(f"\n  total rows in:  {total_expected}")
    print(f"  total accounted: {total_actual} (landed + rejected)")
    if total_actual != total_expected:
        ok = False
        print("  MISMATCH: some rows are unaccounted for (neither landed nor rejected)")

    print("\nRESULT:", "agrees -- every row landed or was rejected exactly where "
          "the independent oracle expected" if ok else "DISAGREES -- see mismatches above")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
