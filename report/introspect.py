"""Discovers a Postgres table's primary key and column list via
information_schema -- never a hand-maintained list. This is the mechanism
that lets report/capture.py build its comparison queries without any
per-domain SQL: point it at a different job's fact/reject tables and it
introspects those instead, no code change required.

Reached directly over each stack's published port with psycopg2, the same
trick the reference repo's api/app.py already uses for its
/export/rejects.csv endpoint (host-gateway + published port, never joining
the other stack's Docker network) -- simpler than that repo's own
postgres_fdw-based comparison, and sufficient here because this tool runs as
a one-shot report generator, not a live-joining dashboard.
"""
from __future__ import annotations

import dataclasses

import psycopg2
import psycopg2.extras


@dataclasses.dataclass
class TableSchema:
    schema: str
    table: str
    columns: list[str]
    primary_key: list[str]

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"


def connect(db: dict):
    """db: a job.yml source_db/destination_db block."""
    return psycopg2.connect(
        host=db["host"], port=db["port"], dbname=db["dbname"],
        user=db["user"], password=_password(db),
    )


def _password(db: dict) -> str:
    import os
    ref = db.get("password_ref")
    if ref:
        return os.environ[ref]
    return db["password"]  # only for tests that pass one inline


def _split_table(qualified: str) -> tuple[str, str]:
    if "." in qualified:
        schema, table = qualified.split(".", 1)
        return schema, table
    return "public", qualified


def describe_table(conn, qualified_name: str) -> TableSchema:
    """Primary key columns (in ordinal position) and the full column list
    for one table, read straight from information_schema -- no adapters.yml,
    no per-domain literal anywhere in this function."""
    schema, table = _split_table(qualified_name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = %s AND table_name = %s
             ORDER BY ordinal_position
            """,
            (schema, table),
        )
        columns = [r[0] for r in cur.fetchall()]

        cur.execute(
            """
            SELECT kcu.column_name
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
             WHERE tc.table_schema = %s AND tc.table_name = %s
               AND tc.constraint_type = 'PRIMARY KEY'
             ORDER BY kcu.ordinal_position
            """,
            (schema, table),
        )
        pk = [r[0] for r in cur.fetchall()]

    if not columns:
        raise ValueError(f"{qualified_name}: no such table (or it has no columns) "
                          f"-- check job.yml's `compare:` block")
    return TableSchema(schema=schema, table=table, columns=columns, primary_key=pk)
