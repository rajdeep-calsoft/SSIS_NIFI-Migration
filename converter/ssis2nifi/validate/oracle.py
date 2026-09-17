"""An independent answer to "what should this package do to this data?"

WHY THIS FILE HAS TO EXIST, AND WHY IT MUST NOT IMPORT `emit`
---------------------------------------------------------------
Tiers 0-3 (see DIAGRAM.md picture 4) all check that the generated flow is
*internally consistent* -- well-formed, importable, valid. None of them check
that it is *correct*. A converter that has a bug in both the generator and its
own test fixtures would pass all four and still be wrong.

The only way to catch that is to compute the expected answer a SECOND time,
by a route that shares no code with `emit/flowdef.py` and does not go anywhere
near NiFi's `DatabaseRecordLookupService` or `LookupRecord`. This module is
that second route: it reads the same two facts the generated flow's lookups
would use -- the reference table's rows, and each row's join key -- and
reproduces the "does this key exist" decision with a plain Python dict.

If this module agreed with the generator because it copied the generator's
logic, it would prove nothing. It deliberately does not import `emit` --
`tests/unit/test_oracle.py::test_the_oracle_does_not_import_emit` checks this.

What it does NOT do: talk to a database, touch a filesystem, or know NiFi
exists. `validate/behavior.py` supplies live data from outside; this module
only computes the expected outcome from an SSIS package's OWN declared rules
-- `NoMatchBehavior`, `JoinToReferenceColumn`, `SqlCommand` -- decoded into the
`derived` dict by `catalog/derive.py`.
"""

from __future__ import annotations

from typing import Any

from ..ir.model import Package


def lookups_from_package(pkg: Package, identifier_case: str = "") -> list[dict]:
    """Pull out what each Lookup needs to be independently re-checked.

    `identifier_case` mirrors the binding's `identifier_case` -- the same
    fold `emit/flowdef.py`'s `_case_identifiers` applies before the table and
    column names reach Postgres. Applying it here too means this module
    queries the SAME table Postgres actually has, without importing the code
    that does the folding for the generated flow.
    """
    fold = (str.lower if identifier_case == "lower"
            else str.upper if identifier_case == "upper"
            else (lambda s: s))

    out: list[dict] = []
    for df in pkg.dataflows:
        for comp in df.components:
            d = comp.derived
            if not d or "reference_table" not in d:
                continue
            out.append({
                "node": comp.name,
                "input_column": d["input_column"],
                "reference_table": fold(d["reference_table"]),
                "join_column": fold(d["join_column"]),
                "returns": [fold(r) for r in d.get("returns", [])],
                "no_match_fails": bool(d.get("no_match_fails", False)),
            })
    return out


def compute(rows: list[dict[str, Any]], lookups: list[dict],
            references: dict[str, dict[str, dict[str, str]]]) -> list[dict]:
    """The expectation, row by row.

    `references`: {reference_table: {join_key_value: {return_col: value}}} --
    the WHOLE reference table, read independently (behavior.py reads it with
    a plain SQL SELECT, not through a lookup service).

    Applies each lookup in the order the package declares them. A miss on a
    lookup whose `no_match_fails` is true stops the row there -- that is
    `NoMatchBehavior=0`'s actual meaning (DIAGRAM.md picture 5), reproduced
    here from the same declared flag, not copied from the generator's
    behaviour.
    """
    results = []
    for row in rows:
        out = dict(row)
        outcome, rejected_at = "landed", None
        for lk in lookups:
            key = str(row.get(lk["input_column"], ""))
            table = references.get(lk["reference_table"], {})
            match = table.get(key)
            if match is None:
                if lk["no_match_fails"]:
                    outcome, rejected_at = "rejected", lk["node"]
                    break
                continue
            out.update(match)
        results.append({"outcome": outcome, "rejected_at": rejected_at, **out})
    return results
