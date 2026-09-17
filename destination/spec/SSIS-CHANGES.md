# Changes made in the SSIS repository

Everything this project changed in `~/Desktop/SSIS`, so the other developer can
review, take, or reject each item. That repo is **not** under version control
(`git status` there reports *fatal: not a git repository*), so the originals are
kept beside each edited file as `<name>.orig` — `diff file.orig file` shows
exactly what moved.

**No rejection rule was touched.** `_validate_stream` and every threshold in it
(`CURRENCY_OK`, `TS_WINDOW_DAYS`, `HIGH_VALUE_MIN`, `SUSPICIOUS_QTY`, the DLQ
reason strings, the dedupe rule) are byte-for-byte unchanged. The NiFi side bent
to those rules; they did not bend to it.

---

## Changes needed to run the two engines together

### 1. `docker-compose.yml` — Postgres host port 5433 → 5434

Both stacks bound 5433, so they could never be up at the same time. The NiFi
repo has 5433 in its README, CLAUDE.md and `.env.example`; this repo had it in
exactly one place. Everything else here — the Grafana datasource,
`simulator.py`, `dtsx_builder.py` — talks over the Docker network on
`postgres:5432` and is unaffected.

**Only impact:** `psql -h localhost -p 5433` from the host becomes `-p 5434`.
`docker exec ssis-warehouse psql …` is unchanged.

### 2. `docker-compose.yml` — `etl_data` volume → host bind mount

The landing directory lived inside a Docker named volume, so the host could not
put a file there — which made "feed both engines the identical file" impossible.
It is now a bind mount onto this repo's own `./data`, overridable with
`SHARED_DATA`:

```yaml
- ${SHARED_DATA:-/home/rajdeep/Desktop/SSIS/data}:/data
```

Nothing else changed: `LANDING_DIR` is still `/data/landing`, `DATA_DIR` still
`/data`. The directory simply moved from inside Docker to beside the code, so
`ls data/landing` works and the NiFi generator can write there.

### 3. `scripts/stream_schema.sql` + `scripts/shared_dims.sql` — one shared catalogue

The two engines seeded completely different reference data — 18 products /
60 customers here, 120 products / 500 customers there — so every `UNKNOWN_SKU`
and `UNKNOWN_CUSTOMER` verdict differed for reasons that had nothing to do with
the rules.

**This repo's catalogue won.** The seed block moved out of `stream_schema.sql`
into a generated `scripts/shared_dims.sql`, and the NiFi side now loads the same
rows. The products are unchanged — same 18 skus, same prices, same categories.
Customers gained three things:

- `country` and `segment` columns (additive; no rule here reads them, but the
  NiFi flow's customer enrichment does)
- names for `CUST-00051`–`CUST-00060`. The old seed did
  `generate_series(1, 60)` against a **50**-element name array, so the last ten
  customers were created with a `NULL` name.

Regenerate from the NiFi repo with `make dims`; `scripts/orchestrate.py` applies
it after `stream_schema.sql`.

---

## Fixes that make a run repeatable

### 4. `scripts/apply_sql.py` — added a `__main__`

The module had no command-line entry point, so
`python3 scripts/apply_sql.py some.sql` **silently did nothing** — no output, no
error, no statements executed. It now runs the file, reports each failed
statement and exits non-zero if any failed.

### 5. `scripts/reset_stream.sql` — new

A `demo-reset` equivalent, needed because a comparison run has to start from the
same empty state on both engines. Two things otherwise break on a replay:

- `control.stream_file.source_file` is `UNIQUE` and doubles as the watermark
- the dedupe check reads **all** of `stage.clean_sales`, not just the current
  batch, so leftovers turn every replayed line into `DUP_KEY`

Dimensions are deliberately not truncated.

### 6. `ssis_sim/stream_runner.py` — `ON CONFLICT` on the `stream_file` insert

`control.stream_file.source_file` is `UNIQUE` and the insert had no conflict
clause, so re-processing a file of the same name raised instead of re-recording
it. Now it updates the row's counters and `processed_at`.

### 7. `ssis_sim/stream_runner.py` — `line_no` in the `DUP_KEY` payload

`dlq_insert` stored only `{"order_id": oid}` for `DUP_KEY` rows, dropping
`line_no` from exactly the rows whose reason *is* the `(order_id, line_no)` key.
The cross-engine comparison reads `raw_payload->>'line_no'`, so those rows could
not be matched. The payload now carries both.

### 8. `ssis_sim/stream_runner.py` — catalogue lookups hoisted out of the loop

`_product_lookup(cur)` and `_customer_lookup(cur)` each ran a full-table
`SELECT`, and the ingest loop called each of them **twice per line**. On the
99,727-line fixture that is ~400,000 full table scans, and it dominated the run.
They are now memoized per process and read once per file
(`reset_lookup_cache()` is there for after a re-seed).

Behaviour is identical — the reference tables are not written during a batch.

---

## Changes for the LIVE side-by-side

### 9. `docker-compose.yml` — new `stream_runner` service

`stream_daemon()` was already in `stream_runner.py` — a poll-the-landing-dir
loop, complete and working — but nothing ever called it. The engine only ran
when `orchestrate.py` was invoked by hand, so there was no way to watch it work.

A new container runs it:

```yaml
  stream_runner:
    container_name: ssis-engine
    command: ["python", "-m", "ssis_sim.stream_runner"]
```

`builder` is untouched (still `sleep infinity`), so `docker exec ssis-toolbox …`
works exactly as before. **No rule changed** — this runs the same
`_validate_stream` that `orchestrate.py` does.

### 10. `docker-compose.yml` — `orders_gen` moved behind a compose profile

While the two engines are compared, the NiFi repo's generator writes each batch
into **both** landing directories under the same filename. A second generator
here would give this engine data the other one never sees, and every comparison
would be noise.

`orders_gen` now carries `profiles: ["solo"]`, so `docker compose up -d` leaves
it down. To run this stack on its own data as before:

```bash
docker compose --profile solo up -d orders_gen
```

### 11. Nothing was added to this repo's Docker network

Worth stating because it was tried and had to be undone. The NiFi warehouse
now reads this one through `postgres_fdw`, and the obvious way to wire that
was to put its container on this stack's network. **Do not do that.** Both
stacks name their database service `postgres`, so both containers get the
alias `postgres` here, and *this* repo's code starts resolving `postgres` to
the other warehouse — which fails as

```
FATAL:  password authentication failed for user "etl_user"
```

and reads like a credentials problem. The foreign server goes through the host
gateway on the published port 5434 instead, so this stack's networking is
exactly as it was.

### 12. `docker-compose.yml` — container and image names

Two stacks run side by side, and `etl-postgres` (the NiFi one) against
`etl_dw_postgres` (this one) told a reader nothing. Renamed so `docker ps`
sorts into two obvious groups:

| was | now |
|---|---|
| `etl_dw_postgres` | `ssis-warehouse` |
| `etl_dw_stream` | `ssis-engine` |
| `etl_dw_builder` | `ssis-toolbox` |
| `etl_dw_grafana` | `ssis-grafana` |
| `etl_dw_orders_gen` | `ssis-generator-solo` |
| image `ssistonifi-ssis_builder` | `ssis-etl/engine:latest` |

**Only `container_name` changed — the compose SERVICE names are untouched**, so
`simulator.py` connecting to host `postgres` still resolves, and so does
everything else in this repo. Anything of yours that used the old container
names in a `docker exec` needs the new one.

### 13. `deploy/grafana/provisioning/dashboards/ssis_pipeline_health.json` — new

A board titled **"1 - Pipeline Health (SSIS)"**, laid out panel-for-panel like
the NiFi repo's own health board — four stat tiles, a throughput chart beside a
reject-reason chart, the file ledger, and a rejection log — same positions,
same colours.

It reads only this warehouse, through the `Postgres` / `postgres_uid` datasource
that was already here. Nothing about your datasource changed.

The point is that two people demo these separately. A reviewer flipping between
`:3001` and `:3000` should be reading the same picture twice, not learning two
layouts.

It is **generated**, by `scripts/generate_dashboards.py` in the NiFi repo — the
same script that generates that repo's three boards. Hand edits are lost on the
next `make dashboards`; send changes to that script instead.

Its last panel, **"Rejection log — every record this engine refused"**, reads
`control.dlq_errors` and lifts `line_no`, `sku`, `qty`, `unit_price` and
`currency` out of `raw_payload`. On `DUP_KEY` rows those five come back blank,
because `dlq_insert` stores only `{"order_id": …}` there — see finding 7. The
NiFi board's twin panel shows the same nine columns from `quarantine_records`.

### 16. `deploy/grafana/provisioning/dashboards/ssis_stream.json` — rewritten

Your **"SSIS Streaming ETL - Live"** board is now generated too, as
**"2 - Business Live (SSIS)"**, so that it mirrors the NiFi repo's own
*2 - Business Live* the way the two health boards already mirror each other.
**The uid is unchanged** (`ssis-stream-live`), so every existing link and
bookmark still opens.

Your original is at `deploy/grafana/ssis_stream.json.pre-mirror` — outside the
provisioning folder, so Grafana does not load it, but one `cp` from being back:

```bash
cp deploy/grafana/ssis_stream.json.pre-mirror \
   deploy/grafana/provisioning/dashboards/ssis_stream.json
```

Twenty-one panels became seven. What went, and why — three of these are bugs,
not preferences:

| Removed | Why |
|---|---|
| *Queue depth* tile, *Current queue depth* gauge, *Queue depth / backlog* trend | the same number, three times, on one screen |
| *Rejection rate %* tile + *Rejection rate % trend* | it is on your Pipeline Health board, once |
| *Lines in vs rejected per minute* | **the title promises a comparison the query never makes** — it selects `sum(lines_in)` only |
| *Accepted vs rejected rows per minute* | same — `sum(lines_clean)` only |
| *ETL run logs — last 30 package results* | **`LIMIT 3`**, not 30 |
| three pie charts, *Recent batches*, *Consumer heartbeat*, *Failed jobs* | reject reasons, batch status and the file ledger are all on your Pipeline Health board |

What it shows now: Orders · Lines · Revenue · Alerts raised, then orders and
lines per minute beside revenue per minute, then the live alert log. Every
measure is one both engines compute the same way, which is what makes the pair
comparable at a glance.

If you want any of the removed panels back, say which — the answer is an edit
to `generate_dashboards.py`, not a hand edit here.

---

## The one rule change — agreed with Rajdeep before it was made

### 14. `ssis_sim/stream_runner.py` — `line_no` must parse as an integer

This is the **only** change to your validation ladder, and it was made on
explicit instruction rather than quietly. Everything else in `_validate_stream`
is untouched: same rules, same thresholds, same order, same reason strings.

Added immediately after the `NULL_CRITICAL` check, before the `qty` parse:

```python
_line_no, err = S._parse_int(d.get("line_no"))
if err:
    return reject("PARSE_ERROR", f"line_no {err}")
```

**Why.** This was finding B below, and it accounted for **100% of the remaining
disagreement between the two engines** — 69 records in one live run, every
other reject reason matching to the exact record. NiFi types `line_no` as
`int` in its reader schema and rejects a bad one at the schema gate, because
`line_no` is half its `order_items` PRIMARY KEY; there was no corresponding
check here, so those rows loaded.

**Why this direction and not the other.** The alternative was to stop NiFi
validating `line_no` so both engines accept `"two"`. That was rejected: NiFi's
`order_items.line_no` is `INTEGER NOT NULL` and part of the primary key, so
accepting it would have meant widening the key to TEXT and storing order lines
with no valid position in their order. Rejecting is the correct behaviour and
this side was the one missing it.

**It also fixes a live bug here.** Rule 14 keys dedupe on `(order_id, line_no)`.
With `line_no` unvalidated, `("ORD-x", "two")` and `("ORD-x", "2")` were two
different order lines, and a blank `line_no` collapsed distinct lines of one
order onto `(order_id, '')` — reporting the second as `DUP_KEY` when it was not
a duplicate at all.

**Deliberately NOT added to the `missing` list.** NiFi does not require
`line_no` to be *present*, it requires it to be an *integer*. An empty
`line_no` must be `PARSE_ERROR` on both sides, not `NULL_CRITICAL`. Putting it
in `missing` would have traded one disagreement for another.

**Verified**, by feeding one 7-record file to both engines through
`make feed` — 3 records with a valid `line_no`, 4 with `"two"`, `"N/A"`, `""`
and `"twelve-hundred"`:

```
 nifi_loaded | ssis_loaded | nifi_rejected | ssis_rejected | verdict
           3 |           3 |             4 |             4 | agree

 order_id            | line_no        | nifi_says   | ssis_says   | agree
 ORD-LNTEST-BAD-001  | two            | PARSE_ERROR | PARSE_ERROR | same
 ORD-LNTEST-BAD-002  | N/A            | PARSE_ERROR | PARSE_ERROR | same
 ORD-LNTEST-BAD-003  |                | PARSE_ERROR | PARSE_ERROR | same
 ORD-LNTEST-BAD-004  | twelve-hundred | PARSE_ERROR | PARSE_ERROR | same
```

### 15. `scripts/apply_sql.py` — a comment-only chunk is not a statement

`split_statements` treated the text after the last `;` as a statement even when
it was nothing but `--` lines. Because change 3 left `stream_schema.sql` ending
in a comment block, that produced one final empty statement, Postgres answered
`can't execute an empty query`, and `tests/conftest.py` asserts that no
statement failed — so **all ten tests errored at setup**. Comments are now
stripped before the emptiness test, at both the `;` boundary and the tail, so
any trailing comment in any file is safe.

That was breakage introduced from the NiFi side, not something you had.

> **Unrelated, still open:** your suite is not reliable while the live stack is
> up. `test_inject_fraud_burst` and `test_inject_clean` pass or fail run to run
> with no code change at all, because the tests assert on `etl_db` while
> `ssis-engine` is writing into it. Ten passed, then two failed on the very next
> run with an unmodified tree. Worth pointing the tests at their own database.

---

## Findings — not changed, for you to decide

These are real, and all of them are in your code rather than your catalogue.
Nothing was patched.

### A. `ssis_catalog.yaml` disagrees with `stream_runner.py` in five places

The catalogue is the document; the runner is what executes. Each of these was
verified against your own pytest suite.

| Rule | Catalogue says | Code does |
|---|---|---|
| 2 | an unreadable file quarantines the batch as `READER_FAILURE` | `READER_FAILURE` is raised only from an `OSError` opening the file (`stream_runner.py:125`) — effectively dead code |
| 1 | a malformed file is rejected whole | read line by line; each bad line is `PARSE_ERROR` and the rest of the batch loads (`tests/test_stream.py:137`) |
| fault catalogue | an empty file → `READER_FAILURE` | stages 0 lines and writes **zero** DLQ rows (`tests/test_stream.py:145`) |
| 13 | `line_total` auto-corrected when drift > 0.02 | nothing is corrected — the original value is persisted and `stage.clean_sales` has no `line_total` column (`stream_runner.py:238`) |
| 15 | `HIGH_VALUE` on `line_total > 500` | true, but it tests the **incoming** `line_total`, not `qty * unit_price`. A large order arriving with a missing `line_total` raises no alert |

### B. `line_no` is never validated — RESOLVED, see change 14 above

> **This one has been actioned**, with Rajdeep's agreement, because it was the
> single cause of every remaining engine disagreement. The description below
> is the original finding, kept so the reasoning is on the record.

`line_no` was not in the required-field list and was checked nowhere, so a line
arriving with `line_no` of `""`, `"two"` or `"N/A"` loaded, carrying the unusable
value into `stage.clean_sales.notes`.

**Measured on the 99,727-line fixture: 249 such rows loaded here.** The NiFi side
rejects those same 249 as `PARSE_ERROR`, because `line_no` is half its
`order_items` primary key. This single difference accounts for **100%** of the
remaining disagreement between the two engines — every other reject reason
matches to the exact record.

Worth deciding, because **rule 14 keys dedupe on `(order_id, line_no)`**: when
`line_no` is blank, distinct lines of one order collapse onto the key
`(order_id, '')`, and the second one is reported as `DUP_KEY` when it is not a
duplicate at all.

### C. Alerts are built before dedupe but flushed after it

The alert list is assembled inside `_validate_stream` and written after the
`DUP_KEY` losers are deleted, so a rejected duplicate can still leave a
`control.stream_alerts` row behind.

### D. The `.dtsx` SQL has drifted from the Python

`pkg_fact_load.dtsx` selects `date_key, customer_key, product_key` from
`stage.clean_sales`, which has no such columns; `pkg_cleanse_validate.dtsx` has
no `WHERE source_file`; `pkg_reconcile.dtsx` compares whole-table counts rather
than per-batch. The packages are generated and never parsed, so nothing breaks —
but they no longer describe the runtime.

### E. `orders_stream/gen/faults.py` — `CAUGHT_BY` names reasons you never emit

It still lists `SCHEMA_INVALID` and `BAD_VALUES`, which are NiFi's old
vocabulary and appear nowhere in this pipeline. (The NiFi side has since retired
both.)

### F. `seed_dims.sql` runs before `public.products` exists

`scripts/orchestrate.py` applies `seed_dims.sql` before `stream_schema.sql`,
which is what creates `public.products`, so every run logs
`relation "public.products" does not exist`. Harmless today — a later step
populates the dimension — but it is noise that hides real warnings.

---

## How to check any of this

```bash
cd ~/Desktop/SSIS
diff docker-compose.yml.orig      docker-compose.yml
diff scripts/stream_schema.sql.orig scripts/stream_schema.sql
diff scripts/orchestrate.py.orig  scripts/orchestrate.py
diff ssis_sim/stream_runner.py.orig ssis_sim/stream_runner.py
diff scripts/apply_sql.py.orig    scripts/apply_sql.py
```

To reject a change, `cp <name>.orig <name>`. Items 1–3 are required for the two
engines to run together; 4–8 are required for a comparison to be repeatable;
9–13 are what make a *live* side-by-side possible.

They live in `docker-compose.yml` and one new dashboard JSON, so they show up
in that file's `.orig` diff, and the JSON can simply be deleted.

---

## What changed on the NiFi side because of your code

For symmetry, since these were caused by reading your runner:

- **Cross-batch `DUP_KEY`.** Your rule 14 builds its dedupe set from *all* of
  `stage.clean_sales`, so a line replayed in a later file is rejected. The NiFi
  flow only deduplicated **within** one file and silently UPSERTed the rest —
  measured at 28 `DUP_KEY` on your side and 0 on its, for one repeated file. It
  now looks each key up in its warehouse before loading, and the two agree to
  the record.
- One residual difference there, recorded rather than hidden: your runner
  processes files strictly in sequence, NiFi is a concurrent dataflow, so two
  identical files landing inside one drain cycle are caught by you and partly
  missed by it. Files spaced further apart than one drain — which is every real
  replay — agree exactly.

---

## One place your code is better, and NiFi is not there yet

Putting both rejection logs side by side made this obvious, so it belongs here
rather than in a drawer.

| | reject rows | with an explanation filled in |
|---|---|---|
| yours (`control.dlq_errors.error_detail`) | 911 | **911** |
| NiFi (`quarantine_records.detail`) | 911 | **102** |

Yours reads `quantity -5`, `missing ['customer_id']`, `sku 'SKU-9945' not in
product catalogue`, `unit_price non-numeric 'twelve-hundred'`. NiFi's is blank
on everything except `PARSE_ERROR`, where its own validator supplies the text.

The verdicts themselves agree completely — same records, same reasons. This is
about the **evidence**, which is the migration's third axis: *does NiFi produce
logs at least as good as SSIS's?* Today, on this column, the answer is no.

It is a NiFi-side fix (the eight `R*. reason …` processors set a reason but not
a detail) and it is on the NiFi side's list. **Nothing is being asked of you
here** — it is recorded so that nobody claims parity that does not exist yet.
