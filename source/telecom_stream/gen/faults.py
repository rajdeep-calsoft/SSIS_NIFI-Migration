"""Fault taxonomy for the telecom CDR generator.

Every fault below produces a record with the SAME JSON type on every field as
a clean record -- never a field that is sometimes a number and sometimes a
string. The reference repo hit a real bug from exactly that kind of
heterogeneity (a field like "N/A" some records, an integer on others) making
NiFi's schema-inferred PutDatabaseRecord throw ("Cannot convert CHOICE, type
must be explicit" -- see destination/generator/gen/build_flow.py's own
mitigation for it there). Designing every fault here as an out-of-domain
VALUE within a stable TYPE (an unknown id, an out-of-range integer) avoids
that whole bug class by construction instead of working around one instance
of it after the fact.

Each fault function returns (record, expected_reason). expected_reason is
None for a record meant to load cleanly -- the generator writes this
alongside the NDJSON so the report can check NiFi's actual disposition
against a ground truth nothing in the converter or the flow computed.

Deliberately NOT modeled: duplicate call_id. pkg_telecom_cdr.dtsx has no
duplicate-detection branch (the reference repo's DUP_KEY case exists because
its package explicitly checks for it) -- generating one here would cause a
raw primary-key violation on the INSERT with no assigned reason on either
engine, which is a data-generation bug, not a pipeline correctness question.
Every call_id this generator issues is therefore globally unique.
"""
from __future__ import annotations

import random

from . import model

REASONS = ["UNKNOWN_SUBSCRIBER", "UNKNOWN_TOWER", "UNKNOWN_PLAN", "BAD_DURATION"]


def _unknown_subscriber(rec: dict, rng: random.Random) -> dict:
    rec["subscriber_id"] = f"SUB{rng.randint(900000, 999999)}"  # outside the seeded 1..500 range
    return rec


def _unknown_tower(rec: dict, rng: random.Random) -> dict:
    rec["tower_id"] = f"TWR{rng.randint(9000, 9999)}"  # outside the seeded 1..30 range
    return rec


def _unknown_plan(rec: dict, rng: random.Random) -> dict:
    rec["plan_code"] = "PLAN_DOES_NOT_EXIST"
    return rec


def _bad_duration(rec: dict, rng: random.Random) -> dict:
    rec["duration_sec"] = rng.choice([-1, -60, 7201, 99999])  # still an int, just out of range
    return rec


_INJECTORS = {
    "UNKNOWN_SUBSCRIBER": _unknown_subscriber,
    "UNKNOWN_TOWER": _unknown_tower,
    "UNKNOWN_PLAN": _unknown_plan,
    "BAD_DURATION": _bad_duration,
}


def make_row(catalog: model.Catalog, rng: random.Random, call_id: str,
             error_rate: float) -> tuple[dict, str | None]:
    """One CDR, clean with probability (1 - error_rate), else one of REASONS
    chosen uniformly. Returns (record, expected_reason)."""
    rec = model.make_clean_record(catalog, rng, call_id)
    if rng.random() >= error_rate:
        return rec, None
    reason = rng.choice(REASONS)
    return _INJECTORS[reason](rec, rng), reason


def expected_cost(rec: dict, catalog: model.Catalog) -> float | None:
    """What Compute Cost's FriendlyExpression computes for a clean record --
    (duration_sec / 60.0) * rate_per_min -- used only to populate the ground
    truth fact_calls row this generator writes into source Postgres."""
    plan = next((p for p in catalog.plans if p["plan_code"] == rec["plan_code"]), None)
    if plan is None:
        return None
    return round((rec["duration_sec"] / 60.0) * float(plan["rate_per_min"]), 4)
