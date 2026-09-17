"""M5: does the generated flow actually produce the RIGHT rows, not just valid ones?

Tiers 0-3 (unit, IR golden, flow.json golden, live import) all pass on a flow
that is well-formed and imports clean. None of them notice a flow that is
well-formed, imports clean, and computes the wrong answer -- the LookupRecord
wired to the wrong key column, say, still validates.

This tier closes that gap by:

  1. reading each Lookup's own reference table directly with a SELECT
     (`oracle.py` never touches this file's imports, and this file never
     imports `emit` -- two independently-written halves of one check);
  2. generating a small batch designed to land on every branch of the
     disposition matrix (DIAGRAM.md picture 5): some rows miss the FIRST
     lookup, some miss the SECOND, some miss neither;
  3. feeding it through the REAL, deployed NiFi flow (this is Tier 4, not a
     substitute for Tier 3 -- it still needs a live NiFi and a live Postgres);
  4. comparing NiFi's actual output against the independently computed
     expectation, row for row.

Matching a landed or rejected row back to the input row it came from uses
`averagerate` as a tracer: the generated batch gives every row a distinct
value, which the pipeline carries through unchanged whether the row lands or
is rejected. A real AdventureWorks feed would not have this property; a
synthetic verification batch is free to.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import time
from typing import Any


class BehaviorError(Exception):
    """Something about the environment stopped the check from running at all."""


# --- building the batch ----------------------------------------------------

def make_batch() -> list[dict[str, Any]]:
    """Rows engineered to hit every cell of the disposition matrix.

    - rows 0-5: known currency, known date -> expected LANDED
    - rows 6-8: unknown currency           -> expected rejected at Lookup Currency Key
    - rows 9-11: known currency, unknown date -> expected rejected at Lookup Date Key

    `AverageRate` is the tracer: 9.000 + row index / 1000, distinct per row and
    well outside the 0.5-1.6 range M4's own demo batch happened to land in --
    so re-running this against a database still holding M4's rows can't
    collide with them by coincidence.
    """
    BASE = 9.0
    good_currencies = ["GBP", "EUR", "USD"]
    good_dates = ["2014-01-01", "2014-01-02", "2013-12-31"]
    rows = []
    for i in range(6):
        rows.append({"AverageRate": round(BASE + i / 1000, 3),
                     "CurrencyID": good_currencies[i % 3],
                     "CurrencyDate": good_dates[i % 3],
                     "EndOfDayRate": round(BASE + 0.1 + i / 1000, 3)})
    for i in range(6, 9):
        rows.append({"AverageRate": round(BASE + i / 1000, 3),
                     "CurrencyID": "ZZZ",
                     "CurrencyDate": good_dates[i % 3],
                     "EndOfDayRate": round(BASE + 0.1 + i / 1000, 3)})
    for i in range(9, 12):
        rows.append({"AverageRate": round(BASE + i / 1000, 3),
                     "CurrencyID": good_currencies[i % 3],
                     "CurrencyDate": "2099-06-15",
                     "EndOfDayRate": round(BASE + 0.1 + i / 1000, 3)})
    return rows


def write_flat_file(rows: list[dict], columns: list[str], column_delim: str,
                     row_delim: str, path: pathlib.Path) -> None:
    """The exact format `derive._flat_file_source` decoded -- see IR
    `derived.column_delimiter` / `derived.row_delimiter` for corpus/packages/L1.dtsx."""
    lines = [column_delim.join(str(row[c]) for c in columns) for row in rows]
    # Bytes, not write_text: a "\r\n" row delimiter must survive literally --
    # text-mode writes translate it, and NiFi reads the raw bytes off disk.
    path.write_bytes((row_delim.join(lines) + row_delim).encode("utf-8"))


# --- reading real state, independently of the flow --------------------------

def _psql(container: str, db: str, user: str, sql: str) -> str:
    try:
        result = subprocess.run(
            ["docker", "exec", container, "psql", "-U", user, "-d", db,
             "-t", "-A", "-F", "\t", "-c", sql],
            capture_output=True, text=True, timeout=30,
        )
    except FileNotFoundError as exc:
        raise BehaviorError(f"docker not on PATH: {exc}") from exc
    if result.returncode != 0:
        raise BehaviorError(f"psql failed: {result.stderr.strip()}")
    return result.stdout


def read_reference_table(container: str, db: str, user: str,
                          table: str, key_col: str, value_cols: list[str]
                          ) -> dict[str, dict[str, str]]:
    """SELECT the WHOLE table with a plain query. Not the lookup service."""
    cols = ", ".join([key_col] + value_cols)
    out = _psql(container, db, user, f"select {cols} from {table}")
    ref: dict[str, dict[str, str]] = {}
    for line in out.strip("\n").splitlines():
        if not line:
            continue
        parts = line.split("\t")
        ref[parts[0]] = dict(zip(value_cols, parts[1:]))
    return ref


def _tracer(value: Any) -> str:
    """Canonical key for the AverageRate tracer.

    Postgres prints a whole number back as "9", not "9.0" -- str(9.0) in
    Python is "9.0". Comparing those two spellings as dict keys makes a row
    that landed correctly look "missing entirely". Fixed-precision formatting
    on both sides of every comparison avoids the false positive.
    """
    return f"{float(value):.3f}"


def read_landed_rows(container: str, db: str, user: str, table: str,
                      tracer_col: str = "averagerate") -> dict[str, dict[str, str]]:
    out = _psql(container, db, user, f"select * from {table}")
    if not out.strip():
        return {}
    # column order matches the SELECT *; ask information_schema for names
    cols_out = _psql(container, db, user,
        f"select column_name from information_schema.columns "
        f"where table_schema||'.'||table_name = '{table}' order by ordinal_position")
    cols = [c for c in cols_out.strip("\n").splitlines() if c]
    landed: dict[str, dict[str, str]] = {}
    for line in out.strip("\n").splitlines():
        vals = line.split("\t")
        row = dict(zip(cols, vals))
        landed[_tracer(row[tracer_col])] = row
    return landed


def read_rejected_rows(reject_dir: pathlib.Path, source_filename: str,
                        tracer_field: str = "AverageRate") -> dict[str, dict[str, Any]]:
    """Every reject write for this source file, not just one.

    The reject sink now stamps `${filename}-${uuid}` before writing (see
    `emit/flowdef.py`'s `reject_stamp`) so two different lookups missing on
    the same source file get two files instead of the second overwriting the
    first. Reading only `source_filename` exactly would silently miss the
    ones written after it -- the exact bug this function exists to catch.
    """
    rejected: dict[str, dict[str, Any]] = {}
    for path in sorted(reject_dir.glob(f"{source_filename}-*")):
        for record in json.loads(path.read_text()):
            rejected[_tracer(record[tracer_field])] = record
    return rejected


# --- the verdict --------------------------------------------------------

def diff(expected: list[dict], landed: dict[str, dict], rejected: dict[str, dict]
          ) -> dict[str, Any]:
    """Row-by-row agreement between the independent oracle and what NiFi did."""
    mismatches = []
    for row in expected:
        tracer = _tracer(row["AverageRate"])
        in_landed = tracer in landed
        in_rejected = tracer in rejected
        if row["outcome"] == "landed":
            if not in_landed:
                mismatches.append(f"AverageRate={tracer}: expected LANDED, "
                                   f"{'found in rejects instead' if in_rejected else 'missing entirely'}")
            elif in_rejected:
                mismatches.append(f"AverageRate={tracer}: landed AND rejected -- both fired")
        else:
            if not in_rejected:
                mismatches.append(f"AverageRate={tracer}: expected REJECTED at "
                                   f"{row['rejected_at']}, "
                                   f"{'found landed instead' if in_landed else 'missing entirely'}")
            elif in_landed:
                mismatches.append(f"AverageRate={tracer}: landed AND rejected -- both fired")

    return {
        "total": len(expected),
        "expected_landed": sum(1 for r in expected if r["outcome"] == "landed"),
        "expected_rejected": sum(1 for r in expected if r["outcome"] == "rejected"),
        "actual_landed": len(landed),
        "actual_rejected": len(rejected),
        "mismatches": mismatches,
        "agrees": not mismatches,
    }
