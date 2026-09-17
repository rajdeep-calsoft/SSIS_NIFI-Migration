"""Writing batches into the landing directory, and the one-shot demo scenarios."""
from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from .db import connect
from .faults import RECORD_FAULTS, corrupt, malformed_lines
from .model import Catalog, OrderGenerator, diurnal_factor, now_utc


def landing_dir() -> str:
    path = os.getenv("LANDING_DIR", "/landing")
    os.makedirs(path, exist_ok=True)
    return path


def write_batch(lines: list[str], tag: str = "orders") -> str:
    """Write NDJSON atomically.

    NiFi's ListFile watches this directory. Writing straight to the final name
    would let it pick up a half-written file, so we write to .tmp in the SAME
    directory and rename -- rename is atomic within a filesystem, so ListFile
    only ever sees a complete file. (ListFile's Minimum File Age is the second
    line of defence.)
    """
    directory = landing_dir()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    batch_id = uuid.uuid4().hex[:8]
    final = os.path.join(directory, f"{tag}_{stamp}_{batch_id}.json")
    tmp = final + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")
    os.replace(tmp, final)
    return final


def records_to_lines(records: list[dict]) -> list[str]:
    return [json.dumps(r, ensure_ascii=False) for r in records]


def build_batch(gen: OrderGenerator, n_orders: int, error_rate: float,
                rng: random.Random) -> tuple[list[str], dict[str, int]]:
    """A normal batch: good orders with record-level faults sprinkled in."""
    records = gen.make_batch(n_orders)
    stats: dict[str, int] = {}

    for i, record in enumerate(records):
        if rng.random() < error_rate:
            records[i], kind = corrupt(record, rng)
            stats[kind] = stats.get(kind, 0) + 1

    # duplicate_id: replay an earlier order id with a different value, to prove
    # the orders UPSERT key keeps the table correct.
    if rng.random() < error_rate and gen.recent_order_ids:
        dup = dict(records[0])
        dup["order_id"] = rng.choice(gen.recent_order_ids)
        records.append(dup)
        stats["duplicate_id"] = stats.get("duplicate_id", 0) + 1

    return records_to_lines(records), stats


def stream() -> None:
    """Continuous generation. One batch every BATCH_INTERVAL seconds.

    MAX_BATCHES (0 = infinite) bounds the loop so orchestration and tests can
    run a deterministic burst instead of an endless stream.
    """
    interval = float(os.getenv("BATCH_INTERVAL", "10"))
    base_size = int(os.getenv("BATCH_SIZE", "50"))
    error_rate = float(os.getenv("ERROR_RATE", "0.05"))
    max_batches = int(os.getenv("MAX_BATCHES", "0"))
    seed_env = os.getenv("GEN_SEED", "").strip()
    seed = int(seed_env) if seed_env else None

    rng = random.Random(seed)
    conn = connect()
    # The reference tables (products/customers) are created by the SSIS schema
    # init. In a compose stack those may not exist yet, so wait -- a generator
    # that dies on startup is worse than one that waits.
    catalog = None
    for attempt in range(1, 31):
        try:
            catalog = Catalog.from_db(conn)
            break
        except Exception as exc:  # noqa: BLE001
            print(f"[gen] catalog not ready ({attempt}/30): {exc}", flush=True)
            conn.close()
            time.sleep(2.0)
            conn = connect()
    if catalog is None:
        raise RuntimeError("catalog never became available")
    conn.close()
    gen = OrderGenerator(catalog, seed=seed)

    print(f"[gen] catalog: {len(catalog.products)} products, "
          f"{len(catalog.customer_ids)} customers", flush=True)
    print(f"[gen] streaming every {interval}s, ~{base_size} orders/batch, "
          f"error_rate={error_rate}, seed={seed}", flush=True)

    batch_no = 0
    while max_batches <= 0 or batch_no < max_batches:
        batch_no += 1
        # traffic follows a daily curve so the dashboards are not a flat line
        n_orders = max(1, int(base_size * diurnal_factor(now_utc()) * rng.uniform(0.85, 1.15)))

        # Every ~20th batch, emit a file-level fault as its OWN file so the
        # reader-failure path stays demoable without wrecking good data.
        if batch_no % 20 == 0:
            path = write_batch(malformed_lines(rng), tag="corrupt")
            print(f"[gen] batch {batch_no}: FILE-LEVEL FAULT (malformed json) -> "
                  f"{os.path.basename(path)}", flush=True)
        elif batch_no % 37 == 0:
            path = write_batch([], tag="empty")
            print(f"[gen] batch {batch_no}: FILE-LEVEL FAULT (empty file) -> "
                  f"{os.path.basename(path)}", flush=True)
        else:
            lines, stats = build_batch(gen, n_orders, error_rate, rng)
            path = write_batch(lines)
            faults = ", ".join(f"{k}={v}" for k, v in sorted(stats.items())) or "none"
            print(f"[gen] batch {batch_no}: {n_orders} orders / {len(lines)} lines -> "
                  f"{os.path.basename(path)} | faults: {faults}", flush=True)

        time.sleep(interval)

    print(f"[gen] finished: {batch_no} batches written to {landing_dir()} "
          f"(MAX_BATCHES={max_batches})", flush=True)


# --------------------------------------------------------------------------
# One-shot demo scenarios: `make inject SCENARIO=<name>`
# --------------------------------------------------------------------------

SCENARIOS = {
    "clean":       "10 known-good orders, zero faults - proves the happy path",
    "fraud_burst": "20 very large orders from one customer - trips HIGH_VALUE alerts",
    "bad_batch":   "20 orders' worth of lines, every one broken - fills the "
                   "quarantine table across all four reject reasons",
    "flood":       "one huge file (FLOOD_ORDERS, default 5000 orders) - a "
                   "throughput burst; NiFi chews ~10k lines in a few seconds",
    "duplicates":  "replays the last batch verbatim - proves UPSERT idempotency",
    "malformed":   "a file of unparseable JSON - exercises the reader-failure path",
    "empty":       "a zero-byte file - the edge case everyone forgets",
    "suspicious":  "orders with absurd line quantities - trips SUSPICIOUS_QTY alerts",
}


def inject(scenario: str) -> None:
    if scenario not in SCENARIOS:
        raise SystemExit(
            f"unknown scenario '{scenario}'. Available:\n  " +
            "\n  ".join(f"{k:<12} {v}" for k, v in SCENARIOS.items())
        )

    rng = random.Random()
    conn = connect()
    catalog = Catalog.from_db(conn)
    conn.close()
    gen = OrderGenerator(catalog)

    if scenario == "clean":
        lines = records_to_lines(gen.make_batch(10))
        path = write_batch(lines, tag="clean")

    elif scenario == "fraud_burst":
        victim = rng.choice(catalog.customer_ids)
        records: list[dict] = []
        for _ in range(20):
            records.extend(gen.high_value_order(customer_id=victim))
        path = write_batch(records_to_lines(records), tag="fraud")
        print(f"[inject] 20 high-value orders all from {victim}", flush=True)

    elif scenario == "suspicious":
        records = []
        for _ in range(15):
            order = gen.make_order()
            for line in order:
                line["qty"] = rng.randint(60, 400)
                line["line_total"] = round(line["unit_price"] * line["qty"], 2)
            records.extend(order)
        path = write_batch(records_to_lines(records), tag="suspicious")

    elif scenario == "bad_batch":
        records = gen.make_batch(20)
        # Every fault chosen here is GUARANTEED to be rejected by the SSIS
        # validate step. (wrong_type is excluded: it randomly picks a field,
        # and that may be line_no, which the pipeline does not validate.)
        broken = []
        for record in records:
            corrupted, _ = corrupt(
                record, rng, kind=rng.choice(
                    ["missing_field", "bad_values", "unknown_sku",
                     "unknown_customer", "late_timestamp", "unicode_currency"]))
            broken.append(corrupted)
        path = write_batch(records_to_lines(broken), tag="bad")
        print(f"[inject] {len(broken)} records, all faulty", flush=True)

    elif scenario == "flood":
        size = int(os.getenv("FLOOD_ORDERS", "5000"))
        lines = records_to_lines(gen.make_batch(size))
        path = write_batch(lines, tag="flood")
        print(f"[inject] {size} orders / {len(lines)} lines in one file - "
              f"watch throughput and queue depth on the health dashboard",
              flush=True)

    elif scenario == "duplicates":
        records = gen.make_batch(15)
        path = write_batch(records_to_lines(records), tag="dup1")
        time.sleep(1)
        path = write_batch(records_to_lines(records), tag="dup2")
        print("[inject] same 15 orders written twice - order count must not double",
              flush=True)

    elif scenario == "malformed":
        path = write_batch(malformed_lines(rng, n=8), tag="corrupt")

    elif scenario == "empty":
        path = write_batch([], tag="empty")

    print(f"[inject] scenario '{scenario}' -> {path}", flush=True)
