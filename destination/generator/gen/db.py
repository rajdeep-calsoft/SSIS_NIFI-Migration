"""Shared Postgres helpers. Everything retries, because in a compose stack
the DB is occasionally still coming up when we first reach for it."""
import os
import time

import psycopg


def dsn() -> str:
    return (
        f"host={os.getenv('PGHOST', 'postgres')} "
        f"port={os.getenv('PGPORT', '5432')} "
        f"dbname={os.getenv('PGDATABASE', 'etldemo')} "
        f"user={os.getenv('PGUSER', 'etl')} "
        f"password={os.getenv('PGPASSWORD', 'etlpass')}"
    )


def connect(retries: int = 30, delay: float = 2.0) -> psycopg.Connection:
    """Connect, waiting for Postgres to accept connections."""
    last = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg.connect(dsn(), autocommit=True)
            return conn
        except Exception as exc:  # noqa: BLE001 - we genuinely want any failure
            last = exc
            print(f"[db] not ready ({attempt}/{retries}): {exc}", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"Postgres never became available: {last}")
