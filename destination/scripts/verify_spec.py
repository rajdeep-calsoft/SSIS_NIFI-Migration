#!/usr/bin/env python3
"""
Check that the running pipeline actually does what spec/pipeline.yml says.

    python3 scripts/verify_spec.py          # or: make spec-verify

The spec is extracted from the flow definition, so it is guaranteed to match
the JSON. That is not the same as matching *reality* -- a rule can be present
in the flow and still not fire, and a table can exist and still be empty. This
walks the six sections of spec/CONFORMANCE.md against the live database and
reports pass/fail for each.

Exit code 0 when every check passes, 1 otherwise, so it can gate a commit.

Some checks need data to have flowed. If a section reports NO DATA, run
`make inject SCENARIO=<name>` as the message suggests and try again -- that is
missing evidence, not a failure.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
SPEC = ROOT / "spec" / "pipeline.yml"

GREEN, RED, YELLOW, DIM, OFF = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"

results: list[tuple[str, str]] = []


def sql(query: str) -> list[list[str]]:
    """Run one query in the warehouse and return rows as lists of strings."""
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "etl", "-d", "etldemo", "-tAF|", "-c", query],
        cwd=ROOT, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip() or "psql failed")
    return [line.split("|") for line in out.stdout.strip().splitlines() if line]


def ok(msg: str) -> None:
    print(f"  {GREEN}ok{OFF}    {msg}")
    results.append(("ok", msg))


def bad(msg: str, hint: str = "") -> None:
    print(f"  {RED}FAIL{OFF}  {msg}" + (f"\n        {DIM}{hint}{OFF}" if hint else ""))
    results.append(("fail", msg))


def skip(msg: str, hint: str) -> None:
    print(f"  {YELLOW}no data{OFF}  {msg}\n        {DIM}{hint}{OFF}")
    results.append(("skip", msg))


def check_rejects(spec: dict) -> None:
    print("== 1. reject reasons ==")
    declared = set(spec["rejects"])
    seen = {r[0] for r in sql("select distinct reason from quarantine_records;")}
    if not seen:
        skip("no rejected records yet",
             "run: make inject SCENARIO=bad_batch")
        return
    unknown = seen - declared
    if unknown:
        bad(f"reasons in the database that the spec does not declare: {sorted(unknown)}",
            "either the flow gained a reason or pipeline.yml is stale — "
            "run: make spec")
    else:
        ok(f"every reason in the database is declared ({len(seen)} of "
           f"{len(declared)} seen: {', '.join(sorted(seen))})")
    unseen = declared - seen
    if unseen:
        print(f"        {DIM}declared but not yet observed: {sorted(unseen)}{OFF}")


def check_targets(spec: dict) -> None:
    print("\n== 2. targets and write modes ==")
    for table, target in spec["targets"].items():
        rows = sql(f"""select string_agg(a.attname, ',' order by
                         array_position(i.indkey, a.attnum))
                       from pg_index i
                       join pg_attribute a
                         on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
                       where i.indrelid = '{table}'::regclass and i.indisprimary;""")
        pk = rows[0][0] if rows and rows[0][0] else ""
        if target["mode"] == "upsert":
            want = ",".join(target["keys"])
            if want == pk:
                ok(f"{table}: upsert keys {want} match the primary key")
            else:
                bad(f"{table}: spec says upsert on {want}, primary key is {pk or '(none)'}",
                    "an upsert key that is not the PK will insert duplicates")
        else:
            ok(f"{table}: insert (surrogate key {pk or 'none'})")


def check_idempotency() -> None:
    print("\n== 3. replay safety ==")
    dupes = sql("""select count(*) from (
                     select order_id, line_no from order_items
                     group by 1, 2 having count(*) > 1) d;""")
    n = int(dupes[0][0])
    if n == 0:
        ok("no duplicate (order_id, line_no) rows — replaying a batch is safe")
    else:
        bad(f"{n} duplicated (order_id, line_no) rows",
            "the upsert key is not being honoured")


def check_alerts(spec: dict) -> None:
    print("\n== 4. alert rules ==")
    declared = {a["name"]: a["severity"] for a in spec["alerts"]}
    seen = {r[0]: r[1] for r in
            sql("select distinct alert_type, severity from alerts;")}
    if not seen:
        skip("no alerts raised yet",
             "run: make inject SCENARIO=fraud_burst  (and SCENARIO=suspicious)")
        return
    for name, severity in declared.items():
        if name not in seen:
            print(f"        {DIM}declared but not yet observed: {name}"
                  f"  (make inject SCENARIO="
                  f"{'fraud_burst' if name == 'HIGH_VALUE' else 'suspicious'}){OFF}")
        elif seen[name] != severity:
            bad(f"{name}: spec says severity {severity}, database has {seen[name]}")
        else:
            ok(f"{name}: severity {severity} as declared")
    for extra in set(seen) - set(declared):
        bad(f"alert type in the database that the spec does not declare: {extra}")


def check_job_ledger(spec: dict) -> None:
    print("\n== 5. job ledger ==")
    fields = set(spec["observability"]["job_ledger"]["fields"])
    cols = {r[0] for r in sql("""select column_name from information_schema.columns
                                 where table_name = 'job_runs';""")}
    missing = fields - cols
    if missing:
        bad(f"job_runs is missing declared columns: {sorted(missing)}")
    else:
        ok(f"all {len(fields)} declared columns present")

    rows = sql("select count(*) from job_runs;")
    if int(rows[0][0]) == 0:
        skip("no job_runs rows yet", "run: make inject SCENARIO=clean")
        return
    orphan = sql("""select count(*) from orders o
                    where not exists (select 1 from job_runs j
                                      where j.batch_id = o.batch_id);""")
    if int(orphan[0][0]) == 0:
        ok("every loaded order belongs to a batch that has a job_runs row")
    else:
        bad(f"{orphan[0][0]} orders have no job_runs row",
            "a file was processed without being accounted for")


def check_reject_payload() -> None:
    print("\n== 6. rejects keep their payload ==")
    rows = sql("select count(*) from quarantine_records;")
    if int(rows[0][0]) == 0:
        skip("no rejected records yet", "run: make inject SCENARIO=bad_batch")
        return
    blank = sql("""select count(*) from quarantine_records
                   where coalesce(order_id, '') = ''
                     and coalesce(raw_payload, '') = '';""")
    if int(blank[0][0]) == 0:
        ok("every quarantined record carries an order_id or a raw payload")
    else:
        bad(f"{blank[0][0]} quarantined rows carry neither an order_id nor a payload",
            "a reject with no payload cannot be investigated or replayed")

    # Rejection is per LINE, not per order. An order whose second line has an
    # unknown sku keeps its other lines and still produces an order row -- so
    # the same order_id appearing in both tables is correct. The invariant is
    # at line level: one (order_id, line_no) must never be both.
    #
    # DUP_KEY is the one exception, and it is not a leak. A replayed line IS
    # already loaded -- that is precisely why the replay is rejected. Counting
    # it here would make the check fail every time the replay guard works.
    # (The in-file half of the rule still cannot produce this: those records
    # never reach the loader.)
    # The payload has to match too, not just the key. The generator's
    # `duplicate_id` fault deliberately stamps a RECYCLED order_id onto a
    # different record, so one (order_id, line_no) can legitimately name two
    # unrelated lines -- one loaded, the impostor rejected. Measured example:
    # key ...-01551|1 was loaded as SKU-0010 qty 2 and rejected as SKU-0008
    # qty 1 for UNKNOWN_CUSTOMER. That is the fault working, not a leak, and
    # the SSIS engine reaches the same verdict on the same record.
    leaked = sql("""select count(*) from order_items i
                    join quarantine_records q
                      on q.order_id = i.order_id
                     and q.line_no  = i.line_no::text
                     and q.sku      = i.sku
                     and q.qty      = i.qty::text
                     and q.unit_price = i.unit_price::text
                   where q.reason <> 'DUP_KEY';""")
    if int(leaked[0][0]) == 0:
        ok("no line was both rejected and loaded "
           "(DUP_KEY and recycled-id impostors excepted)")
    else:
        bad(f"{leaked[0][0]} order lines were loaded despite being rejected",
            "a line must go one way or the other, never both")

    collisions = sql("""select count(*) from order_items i
                        join quarantine_records q
                          on q.order_id = i.order_id
                         and q.line_no  = i.line_no::text
                       where q.reason <> 'DUP_KEY'
                         and (q.sku, q.qty, q.unit_price)
                             is distinct from (i.sku, i.qty::text,
                                               i.unit_price::text);""")
    if int(collisions[0][0]):
        print(f"        {DIM}{collisions[0][0]} key(s) name two different "
              f"records — the duplicate_id fault recycling an order id{OFF}")

    replayed = sql("""select count(*) from order_items i
                      join quarantine_records q
                        on q.order_id = i.order_id
                       and q.line_no  = i.line_no::text
                     where q.reason = 'DUP_KEY';""")
    if int(replayed[0][0]):
        print(f"        {DIM}{replayed[0][0]} loaded lines were later reported "
              f"DUP_KEY on replay — correct: the row is kept once and the "
              f"replay is recorded{OFF}")

    partial = sql("""select count(distinct o.order_id) from orders o
                     join quarantine_records q on q.order_id = o.order_id;""")
    n = int(partial[0][0])
    if n:
        print(f"        {DIM}{n} orders loaded with at least one line rejected — "
              f"expected, but the SSIS side must agree{OFF}")


def main() -> int:
    if not SPEC.exists():
        print(f"{RED}spec/pipeline.yml not found — run: make spec{OFF}", file=sys.stderr)
        return 1
    spec = yaml.safe_load(SPEC.read_text())

    print(f"verifying the live pipeline against {SPEC.relative_to(ROOT)}\n")
    try:
        check_rejects(spec)
        check_targets(spec)
        check_idempotency()
        check_alerts(spec)
        check_job_ledger(spec)
        check_reject_payload()
    except RuntimeError as exc:
        print(f"\n{RED}cannot reach the warehouse:{OFF} {exc}", file=sys.stderr)
        print("is the stack up?  run: make up", file=sys.stderr)
        return 1

    passed = sum(1 for kind, _ in results if kind == "ok")
    failed = sum(1 for kind, _ in results if kind == "fail")
    skipped = sum(1 for kind, _ in results if kind == "skip")

    print()
    if failed:
        print(f"{RED}{failed} check(s) failed{OFF} ({passed} passed"
              + (f", {skipped} had no data" if skipped else "") + ")")
        return 1
    print(f"{GREEN}all {passed} checks passed{OFF}"
          + (f" ({skipped} skipped for lack of data)" if skipped else "")
          + " — the running pipeline matches its spec")
    return 0


if __name__ == "__main__":
    sys.exit(main())
