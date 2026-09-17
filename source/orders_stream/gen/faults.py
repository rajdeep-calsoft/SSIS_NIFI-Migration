"""Deliberate data faults.

Two tiers, and the split matters:

  RECORD-LEVEL faults still parse as valid JSON. NiFi's reader consumes the
  whole batch, ValidateRecord/LookupRecord/QueryRecord reject the individual
  bad records, and the good records in the same file carry on. This is the
  everyday case, mixed in at ERROR_RATE.

  FILE-LEVEL faults (malformed JSON, empty file) break the reader itself, so
  they take the entire FlowFile down the failure path. Those are emitted as
  their OWN separate files, never mixed into a good batch -- otherwise one
  corrupt line would destroy 50 perfectly good orders and the demo would show
  nothing but carnage.
"""
from __future__ import annotations

import random
from datetime import timedelta

from .model import fmt_ts, now_utc

RECORD_FAULTS = [
    "missing_field",
    "wrong_type",
    "bad_values",
    "unknown_sku",
    "unknown_customer",
    "late_timestamp",
    "unicode_currency",
]

# What each fault should be caught by, for the docs and the demo runbook.
CAUGHT_BY = {
    "missing_field":    "ValidateRecord  -> SCHEMA_INVALID",
    "wrong_type":       "ValidateRecord  -> SCHEMA_INVALID",
    "bad_values":       "QueryRecord     -> BAD_VALUES",
    "unknown_sku":      "LookupRecord    -> UNKNOWN_SKU",
    "unknown_customer": "LookupRecord    -> UNKNOWN_CUSTOMER",
    "late_timestamp":   "QueryRecord     -> BAD_VALUES (order_ts far outside now)",
    "unicode_currency": "QueryRecord     -> BAD_VALUES (currency not INR/USD/EUR/GBP)",
    "duplicate_id":     "loaded, then de-duplicated by the orders UPSERT key",
    "malformed_json":   "reader failure  -> whole file to data/dlq/",
    "empty_file":       "reader failure  -> whole file to data/dlq/",
}


def corrupt(record: dict, rng: random.Random, kind: str | None = None) -> tuple[dict, str]:
    """Return (corrupted copy, fault name)."""
    kind = kind or rng.choice(RECORD_FAULTS)
    r = dict(record)

    if kind == "missing_field":
        r.pop(rng.choice(["customer_id", "order_ts", "sku"]), None)

    elif kind == "wrong_type":
        choice = rng.choice(["qty", "unit_price", "line_no"])
        r[choice] = rng.choice(["two", "N/A", "", "twelve-hundred"])

    elif kind == "bad_values":
        if rng.random() < 0.5:
            r["qty"] = rng.choice([0, -1, -5])
        else:
            r["unit_price"] = round(-abs(r.get("unit_price", 100.0)), 2)
        r["line_total"] = round(float(r["qty"] or 0) * float(r["unit_price"] or 0), 2) \
            if isinstance(r.get("qty"), (int, float)) else 0

    elif kind == "unknown_sku":
        r["sku"] = f"SKU-{rng.randint(9000, 9999)}"

    elif kind == "unknown_customer":
        r["customer_id"] = f"CUST-{rng.randint(90000, 99999)}"

    elif kind == "late_timestamp":
        # half far-future (clock skew), half ancient (replayed backlog)
        delta = timedelta(days=rng.randint(400, 900))
        skewed = now_utc() + delta if rng.random() < 0.5 else now_utc() - delta
        r["order_ts"] = int(skewed.timestamp() * 1000)
        r["order_ts_iso"] = fmt_ts(skewed)

    elif kind == "unicode_currency":
        # note: a plain "inr" would PASS the currency whitelist (upper-cased to
        # INR) -- every value here must be observably wrong.
        r["currency"] = rng.choice(["₹", "Rs.", "€uro", "USD$"])

    else:
        raise ValueError(f"unknown record fault: {kind}")

    return r, kind


def malformed_lines(rng: random.Random, n: int = 3) -> list[str]:
    """Raw text lines that are NOT valid JSON -- these break the reader."""
    samples = [
        '{"order_id": "ORD-BROKEN-1", "customer_id": "CUST-00001", "qty": ',
        '{"order_id" "ORD-BROKEN-2", missing colon}',
        'this line is not json at all',
        '{"order_id": "ORD-BROKEN-3", "unit_price": 12.4,,}',
    ]
    return [rng.choice(samples) for _ in range(n)]
