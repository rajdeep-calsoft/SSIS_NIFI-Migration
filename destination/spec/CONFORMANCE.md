# Conformance checklist

What an engine must do to claim it implements `pipeline-spec/v1`. This is the
checklist SSIS gets graded on, and the answer to *"does NiFi reject the same
records SSIS rejects, for the same reason?"*

Grade by **behaviour**, not by inspecting the implementation. Feed the same input
to both engines and compare what lands in the database.

---

## 1. Reject reasons — 9 required

Every rejected record must be captured with **exactly** one of these reasons.
Spelling matters: these are compared literally. All nine are now shared with
the SSIS engine — `make rules-diff` reports no reason on one side only.

| Reason | Raised when |
|---|---|
| `NULL_CRITICAL` | a required field is absent or an empty string |
| `PARSE_ERROR` | a field is present but cannot be parsed as its type |
| `UNKNOWN_SKU` | `sku` is absent from `products` |
| `UNKNOWN_CUSTOMER` | `customer_id` is absent from `customers` |
| `RANGE_VIOLATION` | `qty` outside 1–999, or `unit_price` outside 0.01–9999.99 |
| `BAD_TIMESTAMP` | `order_ts` more than 365 days from now, either direction |
| `BAD_CURRENCY` | currency is not INR / USD / EUR / GBP |
| `DUP_KEY` | `(order_id, line_no)` has already been seen |
| `READER_FAILURE` | input could not be parsed at all; whole file dead-lettered |

`SCHEMA_INVALID` was **retired** on 2026-09-12. It bundled "the field is not
there" together with "the field is there but unreadable"; the SSIS engine
reports those separately, so one bucket could not be compared against its two.

**The order the rules fire in is part of the contract.** A record breaking two
rules is reported under the first one reached, so a different order is a
different answer. The sequence is fixed as:

```
required fields → qty parses → qty range → price parses → price range
→ sku exists → customer exists → order_ts parses → order_ts window
→ currency → dedupe
```

**Currency is compared case-insensitively.** `inr` and `INR` are the same
currency; case is a formatting difference, not a data error.

**Rejection is per line, not per order.** An order whose second line has an
unknown sku keeps its other lines and still produces an order row with a
smaller total. So the same `order_id` legitimately appears in both `orders`
and the reject table. The invariant is at line level: one
`(order_id, line_no)` must never be both loaded and rejected.

### Measured against the SSIS engine

Both engines are fed one byte-identical file by `make parallel-run`, then
`make capture` / `make compare` diffs the two warehouses on outcomes. Results
on 2026-09-12, after adopting the SSIS ladder:

| Fixture | Lines | NiFi vs SSIS |
|---|---|---|
| `tier1-smoke` | 106 | **no differences** — identical rows loaded, identical rejects |
| `tier2-mixed` | 2,016 | 4 lines differ, all one cause (below) |
| `tier3-bulk` | 99,727 | 249 lines differ (0.25%), same one cause |

**Those differences are now closed.** Every one of them was the `line_no`
cause, and on 2026-09-13 the SSIS ladder gained a `line_no` integer check
(SSIS-CHANGES.md item 14) — the one rule ever changed on that engine. Measured
since, on the live shared stream: **100.00% agreement over 4,388 settled
decisions, 0 disagreements, 319 files**, with all eight reject reasons at
delta 0. `make compare` on a drained pair returned *no differences* across
8,132 loaded line keys, 4,181 order ids and 402 reject keys.

At `tier3-bulk`, **six of the seven reject reasons observed match to the exact
record** — `BAD_CURRENCY` 881/881, `BAD_TIMESTAMP` 845/845, `NULL_CRITICAL`
1,019/1,019, `RANGE_VIOLATION` 887/887, `UNKNOWN_CUSTOMER` 852/852,
`UNKNOWN_SKU` 873/873. The baseline before this work was 9 differences on
`tier1-smoke` alone; `make rules-diff` went from *5 agree · 8 differ* to
*12 agree · 1 differ*, and to *13 agree · 1 differ · 1 missing here* once
`line_no` was added to his ladder.

#### Adopted from the SSIS engine

| Their rule | Was here | Now |
|---|---|---|
| missing field → `NULL_CRITICAL` | `SCHEMA_INVALID` | new `require_fields` stage in front of `ValidateRecord` |
| unreadable field → `PARSE_ERROR` | `SCHEMA_INVALID` | what `ValidateRecord` rejects after that gate |
| `1 <= qty <= 999` | `qty > 0` | adopted |
| `0.01 <= unit_price <= 9999.99` | `unit_price > 0` | adopted |
| `abs(now - order_ts) <= 365 days` | −365 / +1 day | symmetric |
| timestamp checked **before** currency | currency first | reordered |
| replayed line → `DUP_KEY`, **across batches** | absorbed by the UPSERT | `8b` dedupes within a file, `8c` looks the key up in the warehouse; UPSERT kept as backstop |
| `HIGH_VALUE` on `line_total > 500` per line | `SUM(line_total) > 150000` per order | adopted, including testing the *incoming* `line_total` |

The two rules previously marked **contested** are settled. They were only ever
contested because the engines seeded different catalogues — his prices ran
3.20–249.00, this one's to 89,999 INR, so his `unit_price <= 9999.99` rejected
32% of this data and his `line_total > 500` flagged 96% of lines. Both engines
now seed the **same** catalogue (`spec/fixtures/dims.py`, his 18 products and
60 customers), and on it his thresholds behave exactly as intended.

#### Live side-by-side

`make live-compare` puts both engines on ONE generator: each batch is
serialised once and written into both landing directories under the same
filename (`LANDING_DIRS` in `docker-compose.yml`), checksummed on the way out.
Neither engine generates its own data — his `orders_gen` is behind a compose
profile and stays down.

Measured over a continuous run on 2026-09-12: **1,280 orders / 2,450 lines /
142 rejects / 37 alerts on both engines**, with seven of eight reject reasons
identical to the record and the eighth differing only by the `line_no` cause
below.

Two things are configured off during a live comparison, both deliberate:

- **`FILE_FAULTS=0`.** A malformed file is whole-file `READER_FAILURE` here and
  per-line `PARSE_ERROR` there; an empty file writes a FAILED job row here and
  nothing at all there. Both are known, documented divergences, and leaving
  them on puts a permanent expected gap on the live dashboard. Still reachable
  on demand with `make inject SCENARIO=malformed` / `SCENARIO=empty`.
- **`DUP_REPLAY_GAP=10`** in the `duplicates` scenario — see the replay race
  below.

#### Still open

**1. ~~`line_no` is validated here and nowhere there.~~ CLOSED.**

This was the last record-level difference, and it accounted for **100%** of the
remaining disagreement — 249 of 99,727 lines (0.25%) on `tier3-bulk`, and 69
records in one live run, with every other reject reason matching to the exact
record.

`line_no` is a required int in the `order_line` schema and half the
`order_items` primary key, so a line arriving with `line_no` of `""`, `"two"`
or `"N/A"` is rejected here as `PARSE_ERROR`. The SSIS engine did not validate
it anywhere, so the row loaded, carrying the unusable value into
`stage.clean_sales.notes`.

**Closed on the SSIS side, not this one**, on the user's explicit instruction —
the one rule change made to the other engine (`spec/SSIS-CHANGES.md` item 14).
His ladder now parses `line_no` as an integer immediately after `NULL_CRITICAL`
and rejects a bad one as `PARSE_ERROR`.

The other direction was considered and rejected: making this engine accept
`"two"` would mean widening `order_items.line_no` from `INTEGER NOT NULL` and
dropping it from the primary key, storing order lines with no valid position in
their order. It would also have left his own rule 14 broken, since that keys
dedupe on `(order_id, line_no)` — a blank `line_no` collapsed distinct lines of
one order onto a single key, reporting the second as `DUP_KEY` when it was not
a duplicate.

`line_no` is deliberately **not** in his required-field list: this engine does
not require it to be present, it requires it to be an integer, so an empty
`line_no` is `PARSE_ERROR` on both sides rather than `NULL_CRITICAL` on one.

Verified by feeding one 7-record file to both engines (`make feed`): 3 valid
`line_no` loaded on both, 4 broken ones rejected on both, all four
`PARSE_ERROR`, verdict `agree`.

**2. The replay race: sequential runner vs concurrent dataflow.**

His engine processes files strictly one after another, committing each before
reading the next, so its dedupe set is always complete. NiFi is a concurrent
dataflow: a second file can reach `8c. Check replay` while the first file's
rows are still in `PutDatabaseRecord`, and the lookup then misses them.

Measured: two identical files written **1 second** apart gave 31 `DUP_KEY` on
his side and 4 here. The same file replayed after the first had drained gave
**+137 on both** — exactly equal. So the race is bounded by one drain cycle,
and every replay spaced wider than that agrees to the record.

Not closed, because closing it means serialising the flow — giving up the
concurrency that is the reason to use NiFi at all — to match an artifact of how
the other engine is written. The `duplicates` scenario now spaces its two files
by `DUP_REPLAY_GAP` (default 10s) so it demonstrates a *replay* rather than a
concurrent duplicate.

**3. An unparseable line fails the whole file here.** He rejects the *line* as
`PARSE_ERROR` and keeps the rest of the batch; here the file fails as
`READER_FAILURE`. Same input, very different reject counts. This is a real gap
and closing it needs a per-line parse path, not a threshold change.

**4. An empty file is recorded here and ignored there** — accepted, not a gap.
A zero-byte file is dead-lettered here and leaves a `FAILED` job_runs row; his
pipeline stages zero lines and records nothing. Kept on purpose: a file that
arrives and produces nothing must still be accounted for. It changes the file
ledger only, never a record-level verdict.

#### Approximations, declared

Two parse tests are expressed as SQL rather than Python, so they are not
bit-identical to his helpers:

- `_parse_num` accepts `1e3` and strips thousands separators; the SQL test does
  not recognise exponent notation. No generator emits it.
- `_parse_int` accepts leading/trailing whitespace, which the SQL test trims
  explicitly.

Neither has ever produced a difference in a measured run, but they are written
down here rather than left implicit.

**Test**
```sql
select reason, count(*) from quarantine_records group by 1 order by 1;
```
Run per engine on identical input. The reason sets must be identical and the
counts must match.

**A rejected record must never be silently dropped, and never partially loaded.**
The record's payload is preserved in `quarantine_records`, all columns as TEXT —
a record is quarantined *because* its values are wrong, so a typed column would
fail the very insert meant to capture it.

## 2. Targets — 4 tables, correct write modes

| Table | Mode | Keys |
|---|---|---|
| `orders` | upsert | `order_id` |
| `order_items` | upsert | `order_id`, `line_no` |
| `alerts` | insert | — |
| `quarantine_records` | insert | — |

**Test — idempotency.** Replay the same batch twice. `orders` and `order_items`
counts must not change. An engine that double-inserts fails.

## 3. Alert rules — 2 required

| Name | Severity | Scope | Condition |
|---|---|---|---|
| `HIGH_VALUE` | `WARN` | line | `line_total > 500` |
| `SUSPICIOUS_QTY` | `CRITICAL` | line | `qty > 50` |

**Both are per line.** `HIGH_VALUE` was evaluated per *order* here until
2026-09-12 (`SUM(line_total) > 150000`); it now matches the SSIS engine's
grain and threshold. An engine that evaluates either at the wrong grain
produces different counts on the same input, which is exactly what happened:
per-order it raised 0 alerts on a fixture where per-line raised 1.

`HIGH_VALUE` tests the `line_total` that **arrived**, not `qty * unit_price`
recomputed. That is matched deliberately, bug for bug — a line arriving with a
missing `line_total` raises no alert on either engine. Recomputing here would
be more correct and would break the comparison.

**Test**
```sql
select alert_type, severity, count(*) from alerts group by 1,2;
```

## 4. Job ledger — one row per input file

Table `job_runs`, carrying at minimum:

```
batch_id  source_file  started_at  finished_at  duration_ms
records_valid  records_invalid  orders_loaded  status  error_text
```

`records_valid + records_invalid` must equal the records read from the file.
A file that yields nothing usable must still produce a row, with `status` FAILED.

**Test**
```sql
select batch_id, records_valid, records_invalid, status
from v_job_runs_recent order by finished_at desc limit 20;
```

## 5. Enrichment — must add, and must check

| Lookup | Key | Adds |
|---|---|---|
| `products` | `sku` | `category` |
| `customers` | `customer_id` | `country`, `segment` |

The lookup is doing two jobs: attaching columns, **and** proving the reference
exists. An engine that enriches without rejecting misses is not conformant — it
will load orphan rows the other engine rejects.

## 6. Durability

| Property | Requirement |
|---|---|
| Transient database outage | records wait and retry; **no data loss**, no dead-lettering of good data |
| Engine restart mid-batch | resumes without duplicating already-loaded orders |
| Zero-byte input | handled, logged, no crash |
| Unparseable input | whole file preserved in the dead-letter path, unmodified |

**Test**
```bash
docker compose stop postgres && sleep 40 && docker compose start postgres
# orders must resume climbing; zero duplicates; nothing dead-lettered
```

---

## Scoring

An engine conforms when all six sections pass on identical input. Sections 1 and
2 are the ones that decide the SSIS comparison — the rest are table stakes.

Record gaps honestly in that engine's `unmapped` block. A documented gap is a
finding you can plan around; an undocumented one is an incident.

### How to run the grading

```bash
make parallel-run TIER=tier1-smoke     # clean both, feed both the same file
make capture ENGINE=nifi
make capture ENGINE=ssis
make compare                           # exit 0 = the engines agree
```

`make compare` is the verdict: it diffs totals, reject histograms, loaded order
ids, loaded line keys and reject keys, and exits non-zero on any difference.
The Grafana board *"NiFi vs SSIS — same data, same answer?"* shows the same
comparison as pictures; use it to see the shape, and `make compare` to name the
rows.

`make rules-diff` answers the question that comes first — *are the two engines
even trying to do the same thing?* — and can be run before the other engine has
ever executed.
