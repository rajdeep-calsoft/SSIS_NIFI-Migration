"""The telecom CDR record model + the reference-data catalog rows are drawn
from. Column names here are exactly the ones jobs/telecom_cdr's .dtsx source
columns declare (see generate_pkg_telecom_cdr.py's `source_col` calls) --
kept in sync by hand since this generator and that package are both
hand-authored artifacts describing the same file format, not by any shared
code (there is deliberately no shared code between "what the SSIS side
expects" and "what the file actually contains": that's what a schema
mismatch would need to look like to be caught here at all).
"""
from __future__ import annotations

import dataclasses
import random
import time

import psycopg2
import psycopg2.extras


@dataclasses.dataclass
class Catalog:
    subscribers: list[dict]
    towers: list[dict]
    plans: list[dict]

    @classmethod
    def load(cls, conn) -> "Catalog":
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT subscriber_id, plan_code, status FROM dim_subscriber")
            subscribers = cur.fetchall()
            cur.execute("SELECT tower_id FROM dim_cell_tower")
            towers = cur.fetchall()
            cur.execute("SELECT plan_code, rate_per_min, currency FROM dim_plan")
            plans = cur.fetchall()
        return cls(subscribers=subscribers, towers=towers, plans=plans)


CALL_TYPES = ["voice", "sms", "data"]


def make_clean_record(catalog: Catalog, rng: random.Random, call_id: str) -> dict:
    sub = rng.choice(catalog.subscribers)
    tower = rng.choice(catalog.towers)
    duration = rng.randint(0, 3600)
    return {
        "call_id": call_id,
        "subscriber_id": sub["subscriber_id"],
        "tower_id": tower["tower_id"],
        "plan_code": sub["plan_code"],
        "call_type": rng.choice(CALL_TYPES),
        "call_ts": int(time.time() * 1000) - rng.randint(0, 86_400_000),
        "duration_sec": duration,
    }
