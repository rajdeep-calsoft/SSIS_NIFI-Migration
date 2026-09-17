#!/usr/bin/env python3
"""
Build a frozen dataset that BOTH engines consume, byte for byte.

    python3 spec/fixtures/make_fixture.py tier1-smoke

Why frozen and not reproducible: `OrderGenerator.run_token` is a `uuid4` and
`make_order` reads the wall clock, so the same seed does NOT give the same
file. Rather than fight that, a dataset is generated ONCE and kept. The file
is the contract; `make compare` is only meaningful when both engines read the
identical bytes.

The catalogue comes from `dims.py`, not from a database -- so a fixture can
never reference a sku or customer that the shared seed does not contain.

Timestamps are generated relative to a fixed BASE_TS recorded in the sidecar
`.meta.json`. `make fixture-inject` shifts every order_ts by (now - BASE_TS)
before handing the file to either engine, which keeps the deliberate
`late_timestamp` offsets intact while stopping a stored fixture from ageing
past the 365-day window and failing BAD_TIMESTAMP on both sides at once.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import zlib
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "generator"))

import dims  # noqa: E402  (same directory)
sys.path.insert(0, str(HERE))

import gen.faults as gen_faults  # noqa: E402
from gen.faults import RECORD_FAULTS, corrupt  # noqa: E402
from gen.model import Catalog, OrderGenerator, Product  # noqa: E402

# A fixed instant every fixture is generated against. Chosen well inside the
# +/-365 day window so a rebase is a small shift, not a leap.
BASE_TS = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)


# The `late_timestamp` fault offsets from the wall clock, which would make a
# regenerated fixture differ from the committed one on every run. Pin it to the
# fixture's own base instant; the offsets it produces are preserved by the
# rebase at inject time exactly as before.
gen_faults.now_utc = lambda: BASE_TS


def shared_catalog() -> Catalog:
    products = [Product(sku=sku, unit_price=price, category=cat)
                for sku, price, cat, _name, _mfr in dims.PRODUCTS]
    customer_ids = [row[0] for row in dims.customers()]
    return Catalog(products, customer_ids)


def generator(seed: int, token: str) -> OrderGenerator:
    gen = OrderGenerator(shared_catalog(), seed=seed)
    # The run token is a uuid4 by design, to stop separate `inject` runs from
    # colliding. A fixture is the opposite case: regenerating it must produce
    # the same order ids, or the committed file churns for no reason.
    gen.run_token = token
    return gen


# --------------------------------------------------------------------------
# The tiers
# --------------------------------------------------------------------------

def tier1_smoke(rng: random.Random) -> tuple[list[dict], dict]:
    """50 clean orders, then exactly one record carrying each of the 7 record
    faults, then a verbatim replay of the first order. Small enough to read."""
    gen = generator(seed=1001, token="SMK1")
    records = gen.make_batch(50, ts=BASE_TS)
    faults: dict[str, int] = {}

    # One of each fault, applied to a fresh order so the clean 50 stay clean.
    for kind in RECORD_FAULTS:
        line = gen.make_order(ts=BASE_TS)[0]
        broken, name = corrupt(line, rng, kind=kind)
        records.append(broken)
        faults[name] = faults.get(name, 0) + 1

    # `wrong_type` picks its victim field at random and can land on qty="",
    # which the SSIS ladder catches at step 1 as NULL_CRITICAL, not step 2.
    # So PARSE_ERROR gets an explicit, unambiguous case of its own.
    unreadable = gen.make_order(ts=BASE_TS)[0]
    unreadable["qty"] = "two"
    records.append(unreadable)
    faults["unparseable_qty"] = 1

    # A replayed line: same (order_id, line_no) twice in one file -> DUP_KEY.
    records.append(dict(records[0]))
    faults["duplicate_id"] = 1
    return records, faults


def tier2_mixed(rng: random.Random) -> tuple[list[dict], dict]:
    """1,000 orders at the SSIS side's ERROR_RATE of 0.06, plus a replay."""
    gen = generator(seed=2002, token="MIX2")
    records = gen.make_batch(1000, ts=BASE_TS)
    faults: dict[str, int] = {}

    for i, record in enumerate(records):
        if rng.random() < 0.06:
            records[i], kind = corrupt(record, rng)
            faults[kind] = faults.get(kind, 0) + 1

    replayed = [dict(r) for r in records[:40]]
    records.extend(replayed)
    faults["duplicate_id"] = len(replayed)
    return records, faults


def tier3_bulk(rng: random.Random) -> tuple[list[dict], dict]:
    """50,000 orders. The throughput run -- this is where his two per-line
    catalogue SELECTs show up, and where NiFi's queues actually fill."""
    gen = generator(seed=3003, token="BLK3")
    records = gen.make_batch(50_000, ts=BASE_TS)
    faults: dict[str, int] = {}

    for i, record in enumerate(records):
        if rng.random() < 0.06:
            records[i], kind = corrupt(record, rng)
            faults[kind] = faults.get(kind, 0) + 1
    return records, faults


# The fault kinds a converter-GENERATED flow's Business Rules step actually
# implements (RANGE_VIOLATION, BAD_CURRENCY, UNKNOWN_SKU, UNKNOWN_CUSTOMER).
# Deliberately excludes missing_field/wrong_type/late_timestamp
# (NULL_CRITICAL/PARSE_ERROR/BAD_TIMESTAMP), which generated packages don't
# implement (a documented, deliberate scope cut -- see converter/CLAUDE.md
# known gap #3).
#
# Also deliberately excludes an explicit DUP_KEY replay, unlike tier1_smoke's.
# A generated flow's Sort-dedup step (converted from Microsoft.Sort with
# EliminateDuplicates=true) drops a same-file duplicate SILENTLY -- ROW_NUMBER()
# PARTITION BY ... WHERE dup_rank = 1, no branch for the dropped rows -- while
# SSIS's stream_runner explicitly counts a duplicate as a DUP_KEY reject. Both
# engines end up loading the same final rows, but a duplicate here makes
# records_valid + records_invalid undercount lines_in on the NiFi side, which
# fails v_settled_files' exact-equality check and the file never "settles" on
# the comparison board even though the actual data agrees. Sort emitting a
# genuine DUP_KEY branch (not just a silent drop) would close this properly;
# until then, this tier just doesn't exercise it.
_GENERATED_SUPPORTED_FAULTS = ["bad_values", "unknown_sku", "unknown_customer", "unicode_currency"]


def tier3_bulk_generated(rng: random.Random) -> tuple[list[dict], dict]:
    """50,000 orders, same scale as tier3-bulk, but faults restricted to
    what a converter-GENERATED flow actually supports. tier3-bulk's full
    7-fault mix includes missing_field/wrong_type, which land as an empty
    or non-numeric qty/unit_price -- Business Rules runs as ONE SQL query
    over the whole batch, so a single such value there doesn't just reject
    its own record, it crashes evaluation for every remaining record in the
    file, and since that failure output is unwired in the source package,
    the whole remaining batch (tens of thousands of records) gets silently
    dropped, not quarantined. This tier exists so a generated package can be
    compared against SSIS on data it actually claims to handle."""
    gen = generator(seed=3103, token="BLKG")
    records = gen.make_batch(50_000, ts=BASE_TS)
    faults: dict[str, int] = {}

    for i, record in enumerate(records):
        if rng.random() < 0.06:
            kind = rng.choice(_GENERATED_SUPPORTED_FAULTS)
            records[i], name = corrupt(record, rng, kind=kind)
            faults[name] = faults.get(name, 0) + 1

    return records, faults


TIERS = {
    "tier1-smoke": (tier1_smoke, "50 clean orders + one of each fault + a replay"),
    "tier2-mixed": (tier2_mixed, "1,000 orders at 6% faults + a 40-line replay"),
    "tier3-bulk":  (tier3_bulk,  "50,000 orders at 6% faults - the throughput run"),
    "tier3-bulk-generated": (tier3_bulk_generated,
                              "50,000 orders, faults restricted to what a generated flow supports"),
}


def build(name: str) -> int:
    fn, describe = TIERS[name]
    # zlib.crc32, not hash(): str hashing is salted per process, so hash()
    # would silently give a different fixture on every run.
    records, faults = fn(random.Random(zlib.crc32(name.encode())))

    path = HERE / f"{name}.ndjson"
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    orders = len({r.get("order_id") for r in records})
    meta = {
        "name": name,
        "describe": describe,
        "base_ts": int(BASE_TS.timestamp() * 1000),
        "base_ts_iso": BASE_TS.isoformat(),
        "orders": orders,
        "lines": len(records),
        "faults": dict(sorted(faults.items())),
        "catalogue": {"products": len(dims.PRODUCTS),
                      "customers": dims.N_CUSTOMERS},
    }
    (HERE / f"{name}.meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    print(f"{name}: {describe}")
    print(f"  {orders:,} orders / {len(records):,} lines -> {path.relative_to(ROOT)}")
    print(f"  faults: {', '.join(f'{k}={v}' for k, v in meta['faults'].items()) or 'none'}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("tier", nargs="?", choices=sorted(TIERS), help="which fixture")
    ap.add_argument("--all", action="store_true", help="build every tier")
    args = ap.parse_args()

    if args.all:
        for name in sorted(TIERS):
            build(name)
        return 0
    if not args.tier:
        print("fixtures:")
        for name, (_fn, describe) in sorted(TIERS.items()):
            print(f"  {name:<12} {describe}")
        return 1
    return build(args.tier)


if __name__ == "__main__":
    raise SystemExit(main())
