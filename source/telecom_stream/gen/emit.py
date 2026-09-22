"""Writes one batch: an NDJSON landing file for NiFi to pick up, AND the
ground-truth rows a correctly-behaving SSIS engine would have produced for
the SAME file, loaded straight into source Postgres.

WHY THE GENERATOR ALSO PLAYS "SSIS ENGINE"
-------------------------------------------
The reference repo runs a second, independent process (source/ssis_sim/
stream_runner.py) that re-parses each landing file and re-implements the
validation logic in Python, so the SSIS-side result is genuinely computed,
not copied from the generator's own fault labels. Building a second full
ingest engine for telecom was judged out of proportion to what it would add:
this generator already knows, by construction, exactly which reason (if any)
a correctly-implemented SSIS package would assign to each row -- that's
what makes it useful as an oracle at all (see the reference repo's own
converter/scripts/verify_bulk.py, which uses the identical "the generator
IS the independent oracle" idea). Writing that same, already-known-correct
disposition into source Postgres stands in for running the SSIS engine,
without re-deriving conclusions the generator already reached.

The landing NDJSON, by contrast, carries NO disposition -- it is the exact
same bytes a real SSIS/NiFi engine would have to figure out for itself,
which is what makes comparing NiFi's actual output against this ground
truth a real test and not a tautology.

Atomic writes: write to a temp path in the same directory, then rename --
NiFi's ListFile/GetFile must never see a partially-written file.
"""
from __future__ import annotations

import json
import pathlib
import random
import tempfile
import time
import uuid

import psycopg2

from . import faults, model


def _atomic_write_ndjson(path: pathlib.Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with open(fd, "w") as fh:
            for rec in records:
                fh.write(json.dumps(rec) + "\n")
        # mkstemp always creates at 0600; NiFi's container runs as uid 1000
        # (not this file's owner), so it needs the world-read bit to see it.
        pathlib.Path(tmp).chmod(0o644)
        pathlib.Path(tmp).rename(path)
    finally:
        pathlib.Path(tmp).unlink(missing_ok=True)


def write_batch(conn, catalog: model.Catalog, rng: random.Random, seq_start: int,
                 n_rows: int, error_rate: float, landing_dir: pathlib.Path) -> dict:
    """Generates n_rows CDRs, writes them as one NDJSON landing file, and
    loads the ground-truth disposition into source Postgres. Returns a
    summary dict (batch_id, counts by reason)."""
    batch_id = f"tcdr-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    source_file = f"{batch_id}.ndjson"

    rows: list[dict] = []
    fact_rows: list[tuple] = []
    reject_rows: list[tuple] = []
    usage: dict[str, list[int, int, float]] = {}  # subscriber_id -> [count, duration, cost]
    reason_counts: dict[str, int] = {}

    for i in range(n_rows):
        call_id = f"CDR{seq_start + i:09d}"
        rec, reason = faults.make_row(catalog, rng, call_id, error_rate)
        rows.append(rec)
        if reason is None:
            cost = faults.expected_cost(rec, catalog) or 0.0
            fact_rows.append((
                rec["call_id"], rec["subscriber_id"], rec["tower_id"], rec["plan_code"],
                rec["call_type"], rec["call_ts"], rec["duration_sec"], cost,
                batch_id, source_file,
            ))
            u = usage.setdefault(rec["subscriber_id"], [0, 0, 0.0])
            u[0] += 1
            u[1] += rec["duration_sec"]
            u[2] += cost
        else:
            reject_rows.append((rec["call_id"], reason, batch_id, source_file))
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    started_at = time.time()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO control.job_run_log (batch_id, source_file, started_at, status) "
            "VALUES (%s, %s, to_timestamp(%s), 'RUNNING')",
            (batch_id, source_file, started_at),
        )
        if fact_rows:
            cur.executemany(
                "INSERT INTO fact_calls (call_id, subscriber_id, tower_id, plan_code, "
                "call_type, call_ts, duration_sec, cost, batch_id, source_file) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                fact_rows,
            )
        if reject_rows:
            cur.executemany(
                "INSERT INTO quarantine_cdr (call_id, reason, batch_id, source_file) "
                "VALUES (%s,%s,%s,%s)",
                reject_rows,
            )
        for sub_id, (count, duration, cost) in usage.items():
            cur.execute(
                "INSERT INTO subscriber_daily_usage (subscriber_id, call_count, "
                "total_duration_sec, total_cost, batch_id, source_file) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (sub_id, count, duration, round(cost, 4), batch_id, source_file),
            )
        cur.execute(
            "UPDATE control.job_run_log SET finished_at = now(), records_loaded = %s, "
            "records_rejected = %s, duration_ms = %s, status = 'LOADED' WHERE batch_id = %s",
            (len(fact_rows), len(reject_rows),
             int((time.time() - started_at) * 1000), batch_id),
        )
    conn.commit()

    _atomic_write_ndjson(landing_dir / source_file, rows)

    return {
        "batch_id": batch_id, "source_file": source_file, "rows": n_rows,
        "loaded": len(fact_rows), "rejected": len(reject_rows),
        "reasons": reason_counts,
    }
