-- =====================================================================
-- Wipe the streaming results, keep the dimensions.
--
-- The NiFi engine this pipeline is compared against has `make demo-reset`
-- for the same job. Both sides need one, because a comparison run must
-- start from the same empty state on each engine:
--
--   * control.stream_file.source_file is a UNIQUE watermark, so a replayed
--     fixture filename must not already be recorded;
--   * the dedupe check reads ALL of stage.clean_sales, not just this batch,
--     so leftovers from a previous run turn every line into a DUP_KEY.
--
-- Apply with:
--   docker exec etl_dw_builder python3 /app/scripts/apply_sql.py \
--     /app/scripts/reset_stream.sql
-- =====================================================================

TRUNCATE stage.sales_stage,
         stage.clean_sales,
         dw.fact_sales,
         control.dlq_errors,
         control.etl_run_log,
         control.data_quality_metrics,
         control.reconcile_results,
         control.stream_file,
         control.stream_alerts,
         control.runtime_history
   RESTART IDENTITY;

-- etl_batch is the batch-id sequence the other tables reference; restarting it
-- keeps batch ids small and readable across comparison runs.
TRUNCATE control.etl_batch RESTART IDENTITY CASCADE;

-- The singleton runtime row is updated in place, never inserted, so it is
-- cleared rather than truncated.
--
-- Dimensions (public.products, public.customers, dw.dim_*) are deliberately
-- NOT truncated: they are the catalogue shared with the NiFi engine. This
-- note sits ABOVE the last statement on purpose -- apply_sql.py's splitter
-- hands a trailing comment to the server as an empty query, which errors.
UPDATE control.pipeline_runtime
   SET pending_files = 0, last_batch_id = NULL, last_source_file = NULL,
       last_file_status = NULL, failed_jobs = 0, last_error = NULL
 WHERE singleton = 1;
