-- =====================================================================
-- NiFi E-commerce ETL demo — schema
--
-- Design notes:
--  * Fact tables carry NO hard foreign keys. Referential faults injected
--    by the generator (unknown SKU / customer) must be caught IN THE FLOW
--    and quarantined -- not blow up a whole batch insert at the DB layer.
--  * orders / order_items are UPSERT-keyed so replaying a batch is safe.
--  * Timestamps are timestamptz; NiFi readers are configured with an
--    explicit timestamp format so PutDatabaseRecord binds real Timestamps.
-- =====================================================================

-- ---------- dimensions (seeded once, never written by NiFi) ----------

CREATE TABLE customers (
    customer_id   TEXT PRIMARY KEY,
    full_name     TEXT        NOT NULL,
    email         TEXT        NOT NULL,
    country       TEXT        NOT NULL,
    city          TEXT        NOT NULL,
    segment       TEXT        NOT NULL,   -- RETAIL | PRIME | WHOLESALE
    signup_date   DATE        NOT NULL
);

CREATE TABLE products (
    sku           TEXT PRIMARY KEY,
    product_name  TEXT        NOT NULL,
    category      TEXT        NOT NULL,
    unit_price    NUMERIC(12,2) NOT NULL,
    unit_cost     NUMERIC(12,2) NOT NULL
);

-- ---------- facts (written by the NiFi flow) ----------

CREATE TABLE orders (
    order_id      TEXT PRIMARY KEY,
    customer_id   TEXT        NOT NULL,
    order_ts      TIMESTAMPTZ NOT NULL,
    status        TEXT        NOT NULL,   -- PLACED | PAID | SHIPPED | CANCELLED
    channel       TEXT        NOT NULL,   -- WEB | MOBILE | STORE | PARTNER
    payment_type  TEXT,
    currency      TEXT        NOT NULL,
    country       TEXT,                    -- enriched in-flow by LookupRecord
    segment       TEXT,                    -- enriched in-flow by LookupRecord
    order_total   NUMERIC(14,2) NOT NULL,
    item_count    INTEGER     NOT NULL,
    batch_id      TEXT,
    source_file   TEXT,
    ingest_ts     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_orders_ts        ON orders (order_ts DESC);
CREATE INDEX idx_orders_ingest_ts ON orders (ingest_ts DESC);
CREATE INDEX idx_orders_customer  ON orders (customer_id);
CREATE INDEX idx_orders_batch     ON orders (batch_id);

CREATE TABLE order_items (
    order_id      TEXT        NOT NULL,
    line_no       INTEGER     NOT NULL,
    sku           TEXT        NOT NULL,
    qty           INTEGER     NOT NULL,
    unit_price    NUMERIC(12,2) NOT NULL,
    line_total    NUMERIC(14,2) NOT NULL,
    category      TEXT,                    -- enriched in-flow by LookupRecord
    catalog_price NUMERIC(12,2),           -- catalog price at ingest, to spot mispricing
    batch_id      TEXT,
    ingest_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, line_no)
);
CREATE INDEX idx_items_sku ON order_items (sku);

-- ---------- operational / monitoring tables ----------

-- Records rejected somewhere in the flow. Every payload column is TEXT on
-- purpose: a record is quarantined precisely because its values are wrong,
-- so a typed column (qty INTEGER) would fail the insert of the very rows we
-- are trying to capture. PutDatabaseRecord runs with Ignore Unmatched
-- Field/Column Behavior, so heterogeneous reject shapes all land here.
CREATE TABLE quarantine_records (
    id            BIGSERIAL PRIMARY KEY,
    reason        TEXT,        -- one of the 9 reasons in spec/pipeline.yml; see spec/CONFORMANCE.md
    detail        TEXT,        -- NiFi's own explanation, e.g. which field failed validation
    batch_id      TEXT,
    source_file   TEXT,
    order_id      TEXT,
    line_no       TEXT,
    customer_id   TEXT,
    order_ts      TEXT,
    sku           TEXT,
    qty           TEXT,
    unit_price    TEXT,
    line_total    TEXT,
    currency      TEXT,
    status        TEXT,
    channel       TEXT,
    raw_payload   TEXT,        -- only set for file-level rejects loaded from data/dlq/
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_quarantine_at ON quarantine_records (quarantined_at DESC);

-- In-flow real-time rule hits (written by NiFi from QueryRecord branches).
CREATE TABLE alerts (
    id            BIGSERIAL PRIMARY KEY,
    alert_type    TEXT        NOT NULL,   -- HIGH_VALUE | SUSPICIOUS_QTY
    severity      TEXT        NOT NULL,   -- INFO | WARN | CRITICAL
    order_id      TEXT,
    customer_id   TEXT,
    metric_value  NUMERIC(14,2),
    detail        TEXT,
    batch_id      TEXT,
    alert_ts      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_alerts_ts   ON alerts (alert_ts DESC);
CREATE INDEX idx_alerts_type ON alerts (alert_type);

-- One row per file the pipeline processed: the "ETL job run" record.
CREATE TABLE job_runs (
    batch_id        TEXT PRIMARY KEY,
    source_file     TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ DEFAULT now(),
    duration_ms     BIGINT,
    records_valid   INTEGER DEFAULT 0,
    records_invalid INTEGER DEFAULT 0,
    orders_loaded   INTEGER DEFAULT 0,
    status          TEXT    DEFAULT 'SUCCESS',  -- SUCCESS | PARTIAL | FAILED
    error_text      TEXT
);
CREATE INDEX idx_job_runs_finished ON job_runs (finished_at DESC);

-- NiFi's own health, scraped from its REST API every few seconds.
CREATE TABLE nifi_metrics (
    id              BIGSERIAL PRIMARY KEY,
    sampled_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    component       TEXT        NOT NULL,   -- 'ROOT' or a process-group name
    flowfiles_in    BIGINT,
    flowfiles_out   BIGINT,
    bytes_in        BIGINT,
    bytes_out       BIGINT,
    queued_count    BIGINT,
    queued_bytes    BIGINT,
    active_threads  INTEGER,
    heap_used_mb    NUMERIC(10,2),
    heap_max_mb     NUMERIC(10,2),
    heap_pct        NUMERIC(5,2)
);
CREATE INDEX idx_metrics_at ON nifi_metrics (sampled_at DESC);

-- NiFi bulletins (its error/warning feed) mirrored for Grafana.
CREATE TABLE nifi_bulletins (
    id            BIGINT PRIMARY KEY,      -- NiFi's own bulletin id, dedupes replays
    bulletin_ts   TIMESTAMPTZ NOT NULL,
    level         TEXT,
    source_name   TEXT,
    message       TEXT
);
CREATE INDEX idx_bulletins_ts ON nifi_bulletins (bulletin_ts DESC);
