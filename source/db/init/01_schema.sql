-- Telecom warehouse schema, SOURCE side.
--
-- This Postgres instance plays two roles, matching the reference repo's
-- source/ stack: it holds the reference dimensions a real SSIS package's
-- Lookup components would join against (dim_subscriber, dim_cell_tower,
-- dim_plan), and it holds the tables an SSIS-equivalent run of this job
-- would have produced (fact_calls, quarantine_cdr, subscriber_daily_usage,
-- control.job_run_log) -- computed independently by
-- source/telecom_stream/gen (see its own module docstring for why the
-- generator doubling as the "SSIS engine" is a deliberate, documented
-- simplification, not an oversight).

CREATE TABLE IF NOT EXISTS dim_subscriber (
    subscriber_id text PRIMARY KEY,
    msisdn        text NOT NULL,
    plan_code     text NOT NULL,
    status        text NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_cell_tower (
    tower_id text PRIMARY KEY,
    location text NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_plan (
    plan_code    text PRIMARY KEY,
    rate_per_min numeric(10, 4) NOT NULL,
    currency     text NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_calls (
    call_id       text PRIMARY KEY,
    subscriber_id text NOT NULL,
    tower_id      text NOT NULL,
    plan_code     text NOT NULL,
    call_type     text NOT NULL,
    call_ts       bigint NOT NULL,
    duration_sec  integer NOT NULL,
    cost          numeric(12, 4) NOT NULL,
    batch_id      text,
    source_file   text
);

CREATE TABLE IF NOT EXISTS subscriber_daily_usage (
    subscriber_id      text NOT NULL,
    call_count         integer NOT NULL,
    total_duration_sec integer NOT NULL,
    total_cost         numeric(12, 4) NOT NULL,
    batch_id           text,
    source_file        text
);

CREATE TABLE IF NOT EXISTS quarantine_cdr (
    call_id     text NOT NULL,
    reason      text NOT NULL,
    batch_id    text,
    source_file text,
    quarantined_at timestamptz NOT NULL DEFAULT now()
);

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.job_run_log (
    batch_id         text PRIMARY KEY,
    source_file      text NOT NULL,
    started_at       timestamptz,
    finished_at      timestamptz,
    records_loaded   integer DEFAULT 0,
    records_rejected integer DEFAULT 0,
    duration_ms      bigint,
    status           text NOT NULL DEFAULT 'RUNNING'
);
