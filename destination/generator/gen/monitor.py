"""NiFi self-monitoring: scrape the REST API into Postgres for Grafana.

This is the "how do I monitor it" layer that survives a browser refresh.
Three things get collected every MONITOR_INTERVAL seconds:

  * process-group status  -> throughput, queue depth, active threads
  * system diagnostics    -> JVM heap
  * bulletin board        -> NiFi's own error/warning feed

Plus a sweep of data/dlq/ so file-level rejects (unparseable batches) show up
in quarantine_records alongside the record-level ones -- one table to look at.

The loop must never die: NiFi restarts during a demo are normal, and a monitor
that exits on the first ConnectionError is worse than no monitor at all.
"""
from __future__ import annotations

import os
import time

from .db import connect
from .nifi_api import NiFi


def _mb(value: int | None) -> float | None:
    return round(value / 1024 / 1024, 2) if value else None


def collect_status(nifi: NiFi) -> list[tuple]:
    """Root aggregate plus one row per child process group."""
    status = nifi.get("/flow/process-groups/root/status",
                      params={"recursive": "true"})["processGroupStatus"]
    diag = nifi.get("/system-diagnostics")["systemDiagnostics"]["aggregateSnapshot"]

    heap_used = _mb(diag.get("usedHeapBytes"))
    heap_max = _mb(diag.get("maxHeapBytes"))
    heap_pct = round(100.0 * heap_used / heap_max, 2) if heap_used and heap_max else None

    rows = []

    def row(name: str, snap: dict) -> tuple:
        return (
            name,
            snap.get("flowFilesIn"), snap.get("flowFilesOut"),
            snap.get("bytesIn"), snap.get("bytesOut"),
            snap.get("flowFilesQueued"), snap.get("bytesQueued"),
            snap.get("activeThreadCount"),
            heap_used, heap_max, heap_pct,
        )

    root = status["aggregateSnapshot"]
    rows.append(row("ROOT", root))
    for child in root.get("processGroupStatusSnapshots", []):
        snap = child["processGroupStatusSnapshot"]
        rows.append(row(snap.get("name", "unnamed"), snap))
    return rows


def collect_bulletins(nifi: NiFi) -> list[tuple]:
    board = nifi.get("/flow/bulletin-board")["bulletinBoard"]
    rows = []
    for entry in board.get("bulletins", []):
        bulletin = entry.get("bulletin") or {}
        # NiFi reports bulletin timestamps as a time-of-day string ("14:32:05 UTC"),
        # not a full date, so we stamp arrival time and dedupe on NiFi's own id.
        rows.append((
            entry.get("id"),
            bulletin.get("level"),
            bulletin.get("sourceName") or entry.get("sourceName"),
            (bulletin.get("message") or "")[:4000],
        ))
    return rows


def sweep_dlq(conn, seen: set[str]) -> int:
    """Load file-level rejects from data/dlq/ into quarantine_records.

    Files are left on disk on purpose -- during the demo you want to open the
    actual corrupt file, not just read about it.
    """
    dlq = os.getenv("DLQ_DIR", "/dlq")
    if not os.path.isdir(dlq):
        return 0
    loaded = 0
    for name in sorted(os.listdir(dlq)):
        if name in seen or name.startswith("."):
            continue
        path = os.path.join(dlq, name)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                payload = fh.read(4000)
        except OSError:
            continue
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO quarantine_records (reason, source_file, raw_payload) "
                "VALUES (%s, %s, %s)",
                ("READER_FAILURE", name, payload or "<empty file>"),
            )
        seen.add(name)
        loaded += 1
    return loaded


def run() -> None:
    interval = float(os.getenv("MONITOR_INTERVAL", "5"))
    nifi = NiFi()
    conn = connect()

    seen_dlq: set[str] = set()
    with conn.cursor() as cur:
        cur.execute("SELECT source_file FROM quarantine_records "
                    "WHERE reason = 'READER_FAILURE' AND source_file IS NOT NULL")
        seen_dlq = {r[0] for r in cur.fetchall()}

    print(f"[monitor] polling {nifi.base} every {interval}s "
          f"({len(seen_dlq)} dlq files already recorded)", flush=True)

    consecutive_failures = 0
    while True:
        try:
            rows = collect_status(nifi)
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO nifi_metrics (component, flowfiles_in, flowfiles_out,"
                    " bytes_in, bytes_out, queued_count, queued_bytes, active_threads,"
                    " heap_used_mb, heap_max_mb, heap_pct)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)

                bulletins = collect_bulletins(nifi)
                if bulletins:
                    cur.executemany(
                        "INSERT INTO nifi_bulletins (id, bulletin_ts, level, source_name,"
                        " message) VALUES (%s, now(), %s, %s, %s)"
                        " ON CONFLICT (id) DO NOTHING", bulletins)

            sweep_dlq(conn, seen_dlq)

            if consecutive_failures:
                print(f"[monitor] recovered after {consecutive_failures} failure(s)",
                      flush=True)
                consecutive_failures = 0

        except Exception as exc:  # noqa: BLE001 - the loop must outlive any error
            consecutive_failures += 1
            # Only shout occasionally: a NiFi restart should not spam the logs.
            if consecutive_failures <= 3 or consecutive_failures % 12 == 0:
                print(f"[monitor] poll failed ({consecutive_failures}): "
                      f"{type(exc).__name__}: {exc}", flush=True)
            # Reconnect on a short leash. connect() blocks for its full retry
            # budget, so a long DB outage must not turn this into a stall --
            # and an exhausted budget must not kill the monitor either.
            try:
                if conn.closed:
                    conn = connect(retries=3, delay=2.0)
            except Exception:  # noqa: BLE001
                pass

        time.sleep(interval)
