# spec/ — one pipeline, many engines

This folder describes **what the pipeline does**, separately from **which engine
runs it**. That separation is the whole point: swap the engine, keep the meaning.

```
pipeline.yml          WHAT — engine-neutral. No NiFi vocabulary anywhere.
engines/nifi.yml      HOW  — NiFi's realization, extracted from the live flow
engines/ssis.yml      HOW  — SSIS's realization  (to be written)
mapping/nifi.map.yml  the one human judgement the extractor cannot make
extract_nifi.py       regenerates the two YAMLs from nifi/flow/*.json
CONFORMANCE.md        the checklist a new engine is graded against
compare/              capture each engine's results, then diff them
```

## Why two files and not one

A faithful NiFi dump and a faithful SSIS dump would share no structure. You would
have two unrelated files and nothing unified — no way to ask "do these do the
same thing?".

So the neutral layer holds the meaning, and each engine binding says how that
engine achieves it. `pipeline.yml` is the contract. `engines/*.yml` are the
implementations.

## Regenerating

```bash
make spec          # rewrite both YAMLs
make spec-check    # CI mode: diff only, exit 1 on drift
```

**One-way only.** `flow.json` is the input, never the output. `build_flow.py`
stays the source of truth for the flow itself; nothing here can modify it.

## The safety property

Every processor in `flow.json` must appear in `mapping/nifi.map.yml` — either in a
stage or in the `unmapped` allow-list. If it does not, extraction **refuses**:

```
extraction refused — the flow and the mapping disagree:
  - processor not in mapping/nifi.map.yml: '20. New thing'
```

So the spec cannot quietly fall behind the canvas. Add a processor, and you are
forced to say what it means.

## Expression language

NiFi Expression Language is engine-specific, so the extractor normalizes it into
a small neutral function set that any engine can implement:

| NiFi | Neutral | Meaning |
|---|---|---|
| `${now():toNumber():minus(31536000000)}` | `{{ now_minus_days(365) }}` | one year ago |
| `${now():toNumber():plus(86400000)}` | `{{ now_plus_days(1) }}` | one day ahead |
| `${now():toNumber()}` | `{{ now() }}` | current epoch millis |
| `${batch_id}` | `{{ batch_id }}` | id of the batch being processed |
| `${source_file}`, `${filename}` | `{{ source_file }}` | the input file's name |
| `${record.count}` | `{{ record_count }}` | records in the current unit |

An engine binding must supply all six. They are the only runtime values the
neutral spec depends on.

## What `pipeline.yml` contains

| Section | Meaning |
|---|---|
| `records` | the data contract — `order_line` (13 fields) and `quarantine_record` |
| `lookups` | reference data: `products` by `sku`, `customers` by `customer_id` |
| `stages` | the ordered pipeline, each with an explicit failure contract |
| `alerts` | `HIGH_VALUE` (WARN) and `SUSPICIOUS_QTY` (CRITICAL), with conditions |
| `targets` | the four tables, with write mode and upsert keys |
| `rejects` | the seven reasons a record can be turned away |
| `observability` | the `job_runs` ledger and the dead-letter path |

Every stage names what happens when it fails:

```yaml
- id: enrich_product
  type: lookup
  lookup: products
  on_key: sku
  adds: [category]
  on_fail: { action: reject, reason: UNKNOWN_SKU }
```

That line is the comparison. An engine that drops unknown SKUs silently, or
rejects them under a different name, does not conform — and now you can prove it
rather than argue about it.

## Adding an engine

1. Copy the stage ids from `pipeline.yml`. They are the contract; do not rename.
2. Write `engines/<engine>.yml` with the same top-level shape as `engines/nifi.yml`:
   `apiVersion`, `engine`, `implements: pipeline-spec/v1`, `stages` keyed by
   canonical stage id, and an honest `unmapped` section.
3. Grade it against `CONFORMANCE.md`.

Where an engine cannot do something, say so in `unmapped` rather than omitting
it. A known gap is a finding; a silent one is a bug you discover in production.

## Honest limits

- `pipeline.yml` is **descriptive, not executable**. Nothing here runs a pipeline.
- The SQL is Calcite dialect as NiFi evaluates it. Another engine may need to
  translate it; the semantics are what must match, not the string.
- Canvas layout, backpressure thresholds, scheduling and record reader/writer
  wiring are deliberately excluded — they are engine tuning, not pipeline meaning.
  They are listed in `engines/nifi.yml` under `unmapped`.

## Verifying

Two different questions, two commands.

```bash
make spec-check    # does the committed spec match the flow definition?
make spec-verify   # does the RUNNING pipeline match the spec?
```

`spec-check` is static — it re-extracts from `flow.json` and diffs. `spec-verify`
queries the live warehouse and walks the six sections of `CONFORMANCE.md`:
reject reasons, targets and upsert keys, replay safety, alert severities, the
job ledger, and that no line was both loaded and rejected. Twelve checks; exit 0
only when all pass.

A check that reports `no data` is missing evidence, not a failure — run the
`make inject SCENARIO=...` it names, then try again.

## Comparing two engines

Both stacks bind host port 5433, so they cannot run at the same time. Each is
captured while it is the one running, and the comparison happens on the files
afterwards. That also makes a capture a durable record: last week's NiFi run can
be compared against today's SSIS run without re-running either.

```bash
make capture ENGINE=nifi      # snapshot this pipeline
# ... stop this stack, bring the other engine up ...
make capture ENGINE=ssis
make compare                  # diff the two newest captures
```

The engines store nothing in common — NiFi writes `orders` and
`quarantine_records`, the SSIS side writes `dw.fact_sales` and
`control.dlq_errors` — so the comparison is not table to table. Each engine has
an adapter in `compare/adapters.yml` whose queries return the same answers:

| Answer | Compared as |
|---|---|
| totals | orders, line items, alerts, rejects, files |
| rejects by reason | histogram, per reason |
| alerts by type | histogram, per alert |
| loaded order ids | set difference — who loaded what the other did not |
| reject keys | set difference on `order_id \| line_no \| reason` |

Exit code 0 when the two agree. Adding an engine means adding a block to
`adapters.yml`; nothing else changes.

Captures land in `compare/runs/` and are gitignored — they are generated
evidence, not source.
