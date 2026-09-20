# Architecture

One diagram, covering the whole system: two Docker stacks (`source/`,
`destination/`), one migration tool (`migrator/`), one report engine
(`report/`), tied together by one job definition (`jobs/telecom_cdr/`).

```mermaid
flowchart TB
    subgraph JOB["jobs/telecom_cdr/"]
        DTSX[".dtsx<br/>(real SSIS XML)"]
        YML["job.yml<br/>deployment target + compare: block"]
    end

    subgraph MIG["migrator/  (runs once, at compile time)"]
        direction TB
        P["dtsx/parse.py<br/>XML -&gt; IR"]
        D["catalog/derive.py<br/>+ catalogue/*.yml recipes"]
        F["emit/flowdef.py<br/>IR -&gt; flow.json"]
        B["bindings.py<br/>auto-generates bindings.yml"]
        P --> D --> F
        B -.-> F
    end

    subgraph SRC["source/  :5436"]
        direction TB
        GEN["telecom_stream/gen<br/>generator = independent oracle"]
        SP[("postgres<br/>dims + ground truth<br/>fact_calls / quarantine_cdr /<br/>control.job_run_log")]
        GEN -- "writes ground truth" --> SP
    end

    subgraph DST["destination/  :8085 nifi, :5437 postgres, :3002 grafana"]
        direction TB
        NIFI["NiFi<br/>runs the deployed flow"]
        DP[("postgres<br/>same schema as source<br/>fact_calls / quarantine_cdr /<br/>control.job_run_log")]
        GRAF["Grafana<br/>live dashboard"]
        NIFI -- "writes" --> DP
    end

    subgraph REP["report/  (runs on demand)"]
        direction TB
        INTRO["introspect.py<br/>information_schema"]
        CAP["capture.py<br/>builds SQL from discovered schema"]
        DIFF["diff.py<br/>compares two captures"]
        REND["render.py<br/>HTML + JSON"]
        PSTATE["pipeline_state.py<br/>NiFi REST API"]
        INTRO --> CAP --> DIFF --> REND
        PSTATE --> REND
    end

    DTSX --> P
    YML --> B
    YML -. "nifi_internal_host/port" .-> B
    F -- "deploy()" --> NIFI

    GEN -- "writes NDJSON (no answer)" --> NIFI

    GRAF -- "reads (published port)" --> SP
    GRAF -- "reads (internal DNS)" --> DP

    CAP -- "reads directly, published port" --> SP
    CAP -- "reads directly, published port" --> DP
    PSTATE -- "REST API" --> NIFI
```

## Compile time vs. runtime — the distinction that trips people up

The `.dtsx` and `migrator/` only run **once**, when you execute `migrator
convert` / `migrator deploy` (`make migrate`). After that, NiFi has no idea
SSIS or this repo's compiler ever existed — it just runs the `flow.json`
that got produced.

```
compile time (make migrate)          runtime (make generate-data, always-on)
──────────────────────────           ───────────────────────────────────────
.dtsx ─▶ migrator/ ─▶ flow.json       generator ─▶ landing dir ─▶ NiFi ─▶ postgres
              │                            │
              └─▶ deployed once            └─▶ ground truth written to
                  onto NiFi                    source postgres, every batch
```

## What's dynamic vs. what's config, at a glance

| Layer | Dynamic (discovered) | Config (`job.yml`) |
|---|---|---|
| Migration | every table/column name, component wiring, SQL expressions — all read from the `.dtsx` itself | deployment target (`destination_db`), landing dir, ledger table name |
| Report | primary key + full column list per table (`information_schema`) | which tables to compare (`compare.fact_table`/`reject_table`), which column holds the reject reason |

See `docs/FAQ.md` for the detailed walkthrough of each piece, and
`docs/GENERICITY.md` for the evidence that the same `migrator/` code
converts an unrelated orders-domain package with zero changes.
