-- ============================================================
-- SSIS E2E ETL - Database schema (source / stage / dw / control)
-- Run against etl_db as etl_user.
-- ============================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS control;

-- ------------------------------------------------------------
-- STAGE: raw pingest from flat files, one row per file line.
-- Audit/management columns added by ingest package.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.sales_stage (
    stage_row_id    BIGSERIAL PRIMARY KEY,
    source_file     VARCHAR(255),
    source_line_no  BIGINT,
    order_id        VARCHAR(30),
    customer_name   VARCHAR(120),
    customer_email  VARCHAR(160),
    product_code    VARCHAR(40),
    product_name    VARCHAR(120),
    category        VARCHAR(60),
    quantity        VARCHAR(20),       -- raw as-typed, may be textual/non-numeric
    unit_price      VARCHAR(40),
    discount_pct    VARCHAR(20),
    order_date      VARCHAR(30),       -- raw as-typed, mixed date formats
    ship_date       VARCHAR(30),
    notes           TEXT,
    raw_line        TEXT,              -- full original line for DLQ replay
    parse_status    VARCHAR(30) DEFAULT 'OK',   -- OK | MISALIGNED | ENCODING_REJECT | ENCODING_SALVAGED
    parse_error     TEXT,
    staged_at       TIMESTAMP DEFAULT now()
);

-- Clean + validated sales rows (output of pkg_cleanse_validate, input to dims/fact)
CREATE TABLE IF NOT EXISTS stage.clean_sales (
    clean_id        BIGSERIAL PRIMARY KEY,
    etl_batch_id    BIGINT,
    source          VARCHAR(20),             -- STREAM
    source_file     VARCHAR(255),
    source_row_id   BIGINT,
    order_line_id   VARCHAR(60) NOT NULL,    -- order_id (+line) business key
    order_id        VARCHAR(30) NOT NULL,
    customer_name   VARCHAR(120),
    customer_email  VARCHAR(160),
    product_code    VARCHAR(40),
    product_name    VARCHAR(120),
    category        VARCHAR(60),
    quantity        INTEGER,
    unit_price      NUMERIC(14,4),
    discount_pct    NUMERIC(6,4) DEFAULT 0,
    order_date      DATE,
    ship_date       DATE,
    notes           TEXT,
    quality_status  VARCHAR(20),             -- VALID | CLEANED
    defects         JSONB,                   -- list of applied fixes
    fingerprint     VARCHAR(40),             -- md5 of normalized row
    cleaned_at      TIMESTAMP DEFAULT now()
);

-- ------------------------------------------------------------
-- DW STARS: 1 fact + 3 dims (customer is SCD Type 2)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(20),
    day_of_week     VARCHAR(20),
    day_num         SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key    BIGSERIAL PRIMARY KEY,
    customer_id     VARCHAR(40) NOT NULL,          -- stable business id (email)
    customer_name   VARCHAR(120),
    valid_from      DATE,
    valid_to        DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    etl_batch_id    BIGINT
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key     BIGSERIAL PRIMARY KEY,
    product_code    VARCHAR(40) UNIQUE NOT NULL,  -- business key
    product_name    VARCHAR(120),
    category        VARCHAR(60),
    base_price      NUMERIC(14,4),
    manufacturer    VARCHAR(80),
    is_active       BOOLEAN DEFAULT TRUE,
    etl_batch_id    BIGINT
);

CREATE TABLE IF NOT EXISTS dw.fact_sales (
    fact_id         BIGSERIAL PRIMARY KEY,
    order_line_id   VARCHAR(60) NOT NULL,          -- order_id + line ref
    order_id        VARCHAR(30) NOT NULL,
    date_key        INTEGER NOT NULL,
    customer_key    BIGINT NOT NULL,
    product_key     BIGINT NOT NULL,
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(14,4) NOT NULL,
    discount_pct    NUMERIC(6,4) NOT NULL DEFAULT 0,
    total_price     NUMERIC(18,4) NOT NULL,
    load_timestamp  TIMESTAMP DEFAULT now(),
    etl_batch_id    BIGINT
);

-- ------------------------------------------------------------
-- CONTROL: DLQ / run log / quality metrics
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS control.dlq_errors (
    dlq_id          BIGSERIAL PRIMARY KEY,
    etl_batch_id    BIGINT,
    source          VARCHAR(20),           -- STREAM
    source_file     VARCHAR(255),
    source_row_id   BIGINT,
    order_id        VARCHAR(30),
    error_type      VARCHAR(50) NOT NULL,
    error_detail    TEXT,
    raw_payload     JSONB,           -- full original row, replayable
    detected_at     TIMESTAMP DEFAULT now()
);
ALTER TABLE control.dlq_errors ADD COLUMN IF NOT EXISTS source VARCHAR(20);

CREATE TABLE IF NOT EXISTS control.etl_run_log (
    run_log_id      BIGSERIAL PRIMARY KEY,
    etl_batch_id    BIGINT,
    package_name    VARCHAR(120),
    status          VARCHAR(20),
    rows_in         BIGINT DEFAULT 0,
    rows_staged     BIGINT DEFAULT 0,
    rows_clean      BIGINT DEFAULT 0,
    rows_rejected   BIGINT DEFAULT 0,
    rows_loaded     BIGINT DEFAULT 0,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    duration_ms     BIGINT,
    detail          JSONB
);

CREATE TABLE IF NOT EXISTS control.data_quality_metrics (
    qm_id           BIGSERIAL PRIMARY KEY,
    etl_batch_id    BIGINT,
    package_name    VARCHAR(120),
    metric_name     VARCHAR(80),
    metric_value    BIGINT,
    measured_at     TIMESTAMP DEFAULT now()
);

-- Per-run ETL batch header
CREATE TABLE IF NOT EXISTS control.etl_batch (
    etl_batch_id    BIGSERIAL PRIMARY KEY,
    target_rows     INTEGER,
    started_at      TIMESTAMP DEFAULT now(),
    finished_at     TIMESTAMP
);

-- Reconcile results table
CREATE TABLE IF NOT EXISTS control.reconcile_results (
    rec_id          BIGSERIAL PRIMARY KEY,
    etl_batch_id    BIGINT,
    check_name      VARCHAR(120),
    source_count    BIGINT,
    target_count    BIGINT,
    pass            BOOLEAN,
    detail          JSONB,
    checked_at      TIMESTAMP DEFAULT now()
);

-- Streaming runtime health / ops tile (singleton row maintained by the
-- consumer daemon: queue depth, heartbeat, failed jobs, latest error).
CREATE TABLE IF NOT EXISTS control.pipeline_runtime (
    singleton        SMALLINT PRIMARY KEY CHECK (singleton = 1),
    as_of            TIMESTAMPTZ NOT NULL,
    pending_files    BIGINT DEFAULT 0,
    last_batch_id    BIGINT,
    last_source_file VARCHAR(255),
    last_file_status VARCHAR(20),
    failed_jobs      BIGINT DEFAULT 0,
    last_error       TEXT
);

-- Backlog/health time series (one row per consumer poll; pruned to a few
-- hours so the queue-depth/backlog charts stay cheap).
CREATE TABLE IF NOT EXISTS control.runtime_history (
    hist_id        BIGSERIAL PRIMARY KEY,
    as_of          TIMESTAMPTZ NOT NULL,
    pending_files  BIGINT DEFAULT 0,
    failed_jobs    BIGINT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_runtime_history_ts ON control.runtime_history(as_of);

-- ------------------------------------------------------------
-- Indexes
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_stage_source_file   ON stage.sales_stage(source_file);
CREATE INDEX IF NOT EXISTS ix_stage_order_id      ON stage.sales_stage(order_id);
CREATE INDEX IF NOT EXISTS ix_dim_customer_cur    ON dw.dim_customer(customer_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS ix_dim_product_code    ON dw.dim_product(product_code);
CREATE INDEX IF NOT EXISTS ix_dlq_batch           ON control.dlq_errors(etl_batch_id);
CREATE INDEX IF NOT EXISTS ix_fact_date           ON dw.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_customer       ON dw.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_product        ON dw.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS ix_clean_order_line    ON stage.clean_sales(order_line_id);
CREATE INDEX IF NOT EXISTS ix_clean_batch         ON stage.clean_sales(etl_batch_id);