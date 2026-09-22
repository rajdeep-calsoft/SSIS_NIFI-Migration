"""Builds ONE engine's capture: row counts, the fact table's primary-key set,
and (if configured) the reject table's reason histogram -- entirely from
what report/introspect.py discovered about the tables job.yml's `compare:`
block names. No adapters.yml, no per-domain SQL: this same function runs
against telecom's fact_calls/quarantine_cdr today and would run against a
completely different job's tables tomorrow without a code change.

Capture shape mirrors the reference repo's destination/spec/compare/capture.py
+ diff.py (scalars / histograms / sets, one capture per engine, same key
names on both sides so diff.py can compare like-for-like) -- adapted to
reach Postgres directly with psycopg2 instead of shelling out to
`docker compose exec psql`, so this report needs no knowledge of either
stack's compose project or service names, only the host ports in job.yml.
"""
from __future__ import annotations

import datetime

from . import introspect


def row_count(conn, qualified_table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {qualified_table}")  # noqa: S608 -- table name from job.yml/introspection, not user input
        return cur.fetchone()[0]


def key_set(conn, table: introspect.TableSchema) -> list[str]:
    """Every primary-key tuple, as strings (so JSON-serializable and
    order-independent to compare). Composite keys are joined with a
    separator unlikely to collide with real key values."""
    if not table.primary_key:
        raise ValueError(f"{table.qualified} has no primary key -- cannot build a key set "
                          f"for row-level comparison; add one or compare it as a histogram "
                          f"instead (see job.yml's `compare:` block)")
    cols = ", ".join(table.primary_key)
    with conn.cursor() as cur:
        cur.execute(f"SELECT {cols} FROM {table.qualified}")  # noqa: S608
        return ["|".join(str(v) for v in row) for row in cur.fetchall()]


def reason_histogram(conn, qualified_table: str, reason_column: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(  # noqa: S608 -- table/column from job.yml, not user input
            f"SELECT {reason_column}, count(*) FROM {qualified_table} GROUP BY {reason_column}"
        )
        return {reason: count for reason, count in cur.fetchall()}


def capture(conn, compare_cfg: dict, engine_label: str) -> dict:
    fact = introspect.describe_table(conn, compare_cfg["fact_table"])
    scalars = {"fact_count": row_count(conn, fact.qualified)}
    sets = {"fact_keys": key_set(conn, fact)}
    histograms: dict[str, dict[str, int]] = {}

    reject_table = compare_cfg.get("reject_table")
    if reject_table:
        reason_col = compare_cfg.get("reason_column", "reason")
        scalars["reject_count"] = row_count(conn, reject_table)
        histograms["reject_reasons"] = reason_histogram(conn, reject_table, reason_col)

    return {
        "engine": engine_label,
        "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "fact_table": fact.qualified,
        "fact_primary_key": fact.primary_key,
        "scalars": scalars,
        "histograms": histograms,
        "sets": sets,
    }
