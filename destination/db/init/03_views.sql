-- =====================================================================
-- Analysis views. Grafana panels read these; they are also the fastest
-- way to sanity-check the pipeline from psql during a demo.
-- =====================================================================

-- Live throughput: orders landed per minute, by ingest time (not order time).
CREATE VIEW v_orders_per_minute AS
SELECT date_trunc('minute', ingest_ts) AS minute,
       count(*)                        AS orders,
       sum(order_total)                AS revenue,
       sum(item_count)                 AS items
FROM orders
GROUP BY 1
ORDER BY 1;

-- Revenue split by product category (needs the items -> products join).
CREATE VIEW v_revenue_by_category AS
SELECT p.category,
       count(DISTINCT oi.order_id) AS orders,
       sum(oi.qty)                 AS units,
       sum(oi.line_total)          AS revenue
FROM order_items oi
JOIN products p ON p.sku = oi.sku
GROUP BY p.category
ORDER BY revenue DESC;

CREATE VIEW v_top_skus AS
SELECT oi.sku,
       p.product_name,
       p.category,
       sum(oi.qty)        AS units,
       sum(oi.line_total) AS revenue
FROM order_items oi
LEFT JOIN products p ON p.sku = oi.sku
GROUP BY oi.sku, p.product_name, p.category
ORDER BY revenue DESC;

-- Data-quality rate over time: how much of what arrived was usable.
CREATE VIEW v_quality_by_minute AS
SELECT date_trunc('minute', finished_at) AS minute,
       sum(records_valid)                AS valid,
       sum(records_invalid)              AS invalid,
       CASE WHEN sum(records_valid + records_invalid) = 0 THEN 0
            ELSE round(100.0 * sum(records_invalid)
                       / sum(records_valid + records_invalid), 2)
       END                               AS invalid_pct
FROM job_runs
GROUP BY 1
ORDER BY 1;

-- The "ETL job monitor" table: one row per file the pipeline processed.
--
-- Status is DERIVED here rather than stored, for two reasons. The flow writes
-- job_runs from four different processors that finish in no guaranteed order,
-- so a stored status would race. And a real pipeline has a tolerance: a batch
-- that rejected 2 lines out of 90 is a healthy batch, not a degraded one.
CREATE VIEW v_job_runs_recent AS
SELECT batch_id,
       source_file,
       started_at,
       finished_at,
       duration_ms,
       records_valid,
       records_invalid,
       orders_loaded,
       CASE WHEN records_valid + records_invalid = 0 THEN 0
            ELSE round(100.0 * records_invalid
                       / (records_valid + records_invalid), 1)
       END AS reject_pct,
       CASE
           WHEN status = 'FAILED' THEN 'FAILED'
           -- In flight: job_runs is written by four processors that finish in
           -- no fixed order, so a batch whose rejects landed before its loaded
           -- lines would otherwise flash FAILED on every dashboard refresh.
           -- duration_ms is set only by the orders-loaded path, so a fresh row
           -- without it is still being processed.
           --
           -- The window WAS 30 seconds, which is a bet on how long a batch
           -- takes -- and it was tuned against the generator's ~150-line
           -- files. Feed a 20,000-line file and the bet loses: the reject
           -- branch writes within seconds, the load branch is still running
           -- minutes later, and at t+30s the row falls through to the next
           -- arm of this CASE and reads FAILED. A perfectly healthy file then
           -- shows FAILED, 0 loaded, 100% reject rate on every board, which
           -- is the single most alarming thing this pipeline can display and
           -- it was not true.
           --
           -- Ten minutes instead. The trade-off is the other direction: a
           -- batch that genuinely dies now reads RUNNING for ten minutes
           -- rather than thirty seconds. That is the right way round -- a
           -- false RUNNING is a delay in bad news, a false FAILED sends
           -- someone debugging a pipeline that is working.
           WHEN duration_ms IS NULL
                AND finished_at > now() - interval '10 minutes' THEN 'RUNNING'
           WHEN records_valid = 0 AND records_invalid > 0 THEN 'FAILED'
           WHEN records_invalid = 0                      THEN 'SUCCESS'
           WHEN records_invalid::numeric
                / NULLIF(records_valid + records_invalid, 0) > 0.25
                                                         THEN 'PARTIAL'
           ELSE 'SUCCESS'
       END AS status,
       error_text
FROM job_runs
ORDER BY finished_at DESC
LIMIT 50;

CREATE VIEW v_alerts_recent AS
SELECT a.alert_ts,
       a.alert_type,
       a.severity,
       a.order_id,
       a.customer_id,
       c.full_name,
       a.metric_value,
       a.detail
FROM alerts a
LEFT JOIN customers c ON c.customer_id = a.customer_id
ORDER BY a.alert_ts DESC
LIMIT 200;

-- Single-row "is the pipeline alive right now?" summary for the demo.
CREATE VIEW v_pipeline_summary AS
SELECT
    (SELECT count(*) FROM orders)                                   AS total_orders,
    (SELECT coalesce(sum(order_total), 0) FROM orders)              AS total_revenue,
    (SELECT count(*) FROM order_items)                              AS total_items,
    (SELECT count(*) FROM quarantine_records)                       AS quarantined,
    (SELECT count(*) FROM alerts)                                   AS alerts,
    (SELECT count(*) FROM job_runs)                                 AS job_runs,
    (SELECT count(*) FROM v_job_runs_recent WHERE status = 'FAILED')  AS failed_runs,
    (SELECT count(*) FROM v_job_runs_recent WHERE status = 'PARTIAL') AS partial_runs,
    (SELECT max(ingest_ts) FROM orders)                             AS last_order_ingested,
    (SELECT max(sampled_at) FROM nifi_metrics)                      AS last_metric_sample;

-- ---------------------------------------------------------------------
-- The replay guard the flow looks records up against.
--
-- The SSIS engine's rule 14 rejects a line whose (order_id, line_no) has
-- ALREADY been loaded by an earlier file -- not just one repeated inside the
-- current batch. This pipeline's in-batch dedupe cannot see that far, so
-- `8c. Check replay` does a lookup against this view: a hit means the line is
-- a replay and is rejected as DUP_KEY, exactly as it is over there.
--
-- Why a view and not the table: DatabaseRecordLookupService matches on ONE
-- key column, and the natural key here is composite. The view presents it as
-- a single value. `already_loaded` exists only because the service requires
-- at least one value column to return.
CREATE VIEW v_loaded_line_keys AS
SELECT order_id || '#' || line_no AS order_line_id,
       1                          AS already_loaded
  FROM order_items;
