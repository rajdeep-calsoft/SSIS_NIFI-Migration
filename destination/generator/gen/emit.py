"""Writing batches into the landing directory, and the one-shot demo scenarios."""
from __future__ import annotations

import hashlib
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from .db import connect
from .faults import RECORD_FAULTS, corrupt, malformed_lines
from .model import Catalog, OrderGenerator, diurnal_factor, now_utc


def landing_dirs() -> list[str]:
    """Every inbox a batch must be delivered to.

    One generator feeds BOTH engines. LANDING_DIRS is a comma-separated list;
    LANDING_DIR stays supported for the single-engine case. Writing the same
    bytes into both inboxes is what makes a *live* comparison possible -- the
    alternative (each engine generating its own data) is what made the first
    comparison attempt meaningless.
    """
    raw = os.getenv("LANDING_DIRS", "").strip()
    paths = ([p.strip() for p in raw.split(",") if p.strip()]
             if raw else [os.getenv("LANDING_DIR", "/landing")])
    for path in paths:
        os.makedirs(path, exist_ok=True)
    return paths


def landing_dir() -> str:
    """The primary inbox. Kept for callers that only need one."""
    return landing_dirs()[0]


def write_batch(lines: list[str], tag: str = "orders") -> str:
    """Write NDJSON atomically.

    NiFi's ListFile watches this directory. Writing straight to the final name
    would let it pick up a half-written file, so we write to .tmp in the SAME
    directory and rename -- rename is atomic within a filesystem, so ListFile
    only ever sees a complete file. (ListFile's Minimum File Age is the second
    line of defence.)
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    batch_id = uuid.uuid4().hex[:8]
    name = f"{tag}_{stamp}_{batch_id}.json"

    # Serialise ONCE. Both engines must receive identical bytes, and the
    # filename is the SSIS side's uniqueness key, so it must match too.
    payload = "".join(line + "\n" for line in lines).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()

    written = []
    for directory in landing_dirs():
        final = os.path.join(directory, name)
        tmp = final + ".tmp"
        with open(tmp, "wb") as fh:
            fh.write(payload)
        os.replace(tmp, final)
        written.append(final)

    # Cheap, and it has already caught one broken bind mount: a silently
    # unwritable second inbox would show up as "SSIS is behind", days later.
    for path in written:
        with open(path, "rb") as fh:
            if hashlib.sha256(fh.read()).hexdigest() != digest:
                raise RuntimeError(f"delivered copy differs from source: {path}")

    return written[0]


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
    """Continuous generation. One batch every BATCH_INTERVAL seconds."""
    interval = float(os.getenv("BATCH_INTERVAL", "10"))
    base_size = int(os.getenv("BATCH_SIZE", "50"))
    error_rate = float(os.getenv("ERROR_RATE", "0.05"))
    seed_env = os.getenv("GEN_SEED", "").strip()
    seed = int(seed_env) if seed_env else None

    # File-level faults (a malformed file, an empty file) are the TWO places
    # the two engines are known to disagree by design: NiFi fails the whole
    # file as READER_FAILURE and writes a FAILED job row, the SSIS runner
    # rejects per line and writes nothing at all for an empty file. Left on
    # during a live side-by-side they would put a permanent, expected-but-
    # confusing gap on the comparison dashboard. Off when feeding both
    # engines; still reachable any time via `make inject SCENARIO=malformed`.
    file_faults = os.getenv("FILE_FAULTS", "1").strip().lower() not in (
        "0", "false", "no", "off")

    rng = random.Random(seed)
    conn = connect()
    catalog = Catalog.from_db(conn)
    conn.close()
    gen = OrderGenerator(catalog, seed=seed)

    print(f"[gen] catalog: {len(catalog.products)} products, "
          f"{len(catalog.customer_ids)} customers", flush=True)
    print(f"[gen] streaming every {interval}s, ~{base_size} orders/batch, "
          f"error_rate={error_rate}, seed={seed}", flush=True)
    inboxes = landing_dirs()
    print(f"[gen] delivering every batch to {len(inboxes)} inbox(es): "
          f"{', '.join(inboxes)}", flush=True)
    if not file_faults:
        print("[gen] file-level faults OFF (FILE_FAULTS=0) - the two engines "
              "disagree on those by design", flush=True)

    batch_no = 0
    while True:
        batch_no += 1
        # traffic follows a daily curve so the dashboards are not a flat line
        n_orders = max(1, int(base_size * diurnal_factor(now_utc()) * rng.uniform(0.85, 1.15)))

        # Every ~20th batch, emit a file-level fault as its OWN file so the
        # reader-failure path stays demoable without wrecking good data.
        if file_faults and batch_no % 20 == 0:
            path = write_batch(malformed_lines(rng), tag="corrupt")
            print(f"[gen] batch {batch_no}: FILE-LEVEL FAULT (malformed json) -> "
                  f"{os.path.basename(path)}", flush=True)
        elif file_faults and batch_no % 37 == 0:
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
        broken = []
        for record in records:
            corrupted, _ = corrupt(record, rng, kind=rng.choice(RECORD_FAULTS))
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
        # The gap matters, and 1 second was too short. A REPLAY is a file that
        # arrives after the first one has been loaded; two identical files
        # landing inside one drain cycle are concurrent duplicates, which is a
        # different thing. NiFi's `8c. Check replay` looks the key up in the
        # warehouse, so it can only see file 1's lines once they are committed
        # -- with a 1s gap it caught 4 of 31, with a realistic gap it catches
        # all of them and agrees with the SSIS engine to the record.
        #
        # The SSIS engine does not have this race: it processes files strictly
        # one after another in a loop. That is a genuine architectural
        # difference between a sequential runner and a concurrent dataflow,
        # and it is recorded in spec/CONFORMANCE.md rather than hidden here.
        gap = float(os.getenv("DUP_REPLAY_GAP", "10"))
        records = gen.make_batch(15)
        path = write_batch(records_to_lines(records), tag="dup1")
        print(f"[inject] 15 orders written; replaying them in {gap:g}s", flush=True)
        time.sleep(gap)
        path = write_batch(records_to_lines(records), tag="dup2")
        print("[inject] same 15 orders written twice - order count must not "
              "double, and every replayed line must be reported DUP_KEY",
              flush=True)

    elif scenario == "malformed":
        path = write_batch(malformed_lines(rng, n=8), tag="corrupt")

    elif scenario == "empty":
        path = write_batch([], tag="empty")

    print(f"[inject] scenario '{scenario}' -> {path}", flush=True)
