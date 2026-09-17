-- =====================================================================
-- Read the OTHER engine's warehouse from this one.
--
-- Why this exists
-- ---------------
-- The comparison dashboard used to put two independent queries side by side
-- -- one against this warehouse, one against the SSIS warehouse -- and let the
-- reader compare the numbers by eye. That is fine until the two engines are a
-- few seconds out of step, which they always are: NiFi picks a file up in
-- about a second, his runner polls every five. The board then shows
-- "6.10 K vs 6.19 K" and looks like a disagreement when it is only one file in
-- flight.
--
-- With a foreign-data wrapper both warehouses are reachable in ONE query, so
-- the comparison can do what it should have done all along:
--
--   * restrict every total to the files BOTH engines have finished, which
--     removes the in-flight skew entirely;
--   * FULL OUTER JOIN the two reject sets, so a record rejected by one engine
--     and not the other is a row on the screen, not a subtraction the reader
--     has to do;
--   * show one reject log covering both engines instead of only NiFi's.
--
-- If the SSIS stack is down
-- -------------------------
-- CREATE SERVER and CREATE FOREIGN TABLE never connect, so this file applies
-- cleanly on a cold boot with the other stack absent. Queries against the
-- comparison views WILL fail while it is down, and that is the honest
-- behaviour: the answer to "do the engines agree" is unavailable, not "yes".
-- Everything on the NiFi-only dashboards keeps working.
--
-- Re-apply after the other stack moves:  make fdw
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS ssis_warehouse CASCADE;

-- Reached through the host gateway on his published port 5434, NOT by joining
-- his Docker network. Both stacks name their database service `postgres`, so
-- attaching this container to his network gives two containers the same alias
-- there and his own code starts resolving `postgres` to this one -- which
-- surfaces as "password authentication failed for user etl_user" and looks
-- like a credentials problem when it is a name collision. `ssis-host` is
-- mapped to host-gateway in docker-compose.yml.
CREATE SERVER ssis_warehouse
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'ssis-host', port '5434', dbname 'etl_db');

-- Password comes from the SSIS_POSTGRES_PASSWORD env var (set in
-- docker-compose.yml from .env), never a literal -- this file is committed.
\getenv ssis_pg_password SSIS_POSTGRES_PASSWORD
CREATE USER MAPPING FOR CURRENT_USER
  SERVER ssis_warehouse
  OPTIONS (user 'etl_user', password :'ssis_pg_password');

CREATE SCHEMA IF NOT EXISTS ssis;

-- Foreign tables are declared column by column rather than with IMPORT FOREIGN
-- SCHEMA, because IMPORT has to connect and this file must apply with the
-- other stack down. Only the columns the comparison needs are declared.

CREATE FOREIGN TABLE ssis.dlq_errors (
    dlq_id        bigint,
    source_file   varchar(255),
    order_id      varchar(30),
    error_type    varchar(50),
    error_detail  text,
    raw_payload   jsonb,
    detected_at   timestamp
) SERVER ssis_warehouse OPTIONS (schema_name 'control', table_name 'dlq_errors');

CREATE FOREIGN TABLE ssis.stream_file (
    source_file    varchar(255),
    lines_in       bigint,
    lines_clean    bigint,
    lines_rejected bigint,
    orders_loaded  bigint,
    revenue        numeric(18,4),
    status         varchar(20),
    processed_at   timestamptz
) SERVER ssis_warehouse OPTIONS (schema_name 'control', table_name 'stream_file');

CREATE FOREIGN TABLE ssis.clean_sales (
    order_line_id varchar(60),
    order_id      varchar(30),
    source_file   varchar(255),
    quantity      integer,
    unit_price    numeric(14,4),
    notes         text
) SERVER ssis_warehouse OPTIONS (schema_name 'stage', table_name 'clean_sales');

CREATE FOREIGN TABLE ssis.stream_alerts (
    source_file varchar(255),
    order_id    varchar(30),
    customer_id varchar(40),
    alert_type  varchar(40),
    alert_value numeric(18,4),
    alert_ts    timestamptz
) SERVER ssis_warehouse OPTIONS (schema_name 'control', table_name 'stream_alerts');


-- ---------------------------------------------------------------------
-- Settled files: the ones BOTH engines have finished.
--
-- This is the single most important view here. Every comparison below is
-- restricted to it, so a file that one engine has processed and the other has
-- not simply does not count yet. Without it the live board reports a
-- difference every few seconds that resolves itself, and a reader cannot tell
-- those apart from a real one.
--
-- Both engines record the file under the same name -- the generator writes one
-- batch into both inboxes under one filename -- so the name is the join key.
--
-- "Has a ledger row" is NOT the same as "has finished", and assuming it was
-- is what let a skew through. NiFi is a CONCURRENT dataflow: one input file
-- becomes many FlowFiles that load in parallel, while its single job_runs row
-- is written by one processor at its own moment. `finished_at` cannot settle
-- it either -- the column DEFAULTs to now(), so it is never null and never
-- means "done". The result was a window, a second or two wide, in which a file
-- counted as settled while some of its order_items were still being inserted,
-- and the board showed "Orders -71" against "0 disagreements, 0 in flight" --
-- sampled 14 times, 12 read exactly zero and 2 caught the window.
--
-- Nor can any SINGLE column of job_runs settle it. That row is built by THREE
-- independent upserts on the same batch_id -- "13. Record job run" writes
-- orders_loaded, "16. Record rejects" writes records_invalid, "19. Record
-- loaded lines" writes records_valid -- and whichever branch finishes first
-- CREATES the row, leaving the other columns at their DEFAULT of 0. So a row
-- can sit there reading records_valid = 0 while 99 lines are still loading,
-- and any test of the form "ledger agrees with the rows loaded so far" is
-- satisfied trivially by 0 = 0. That is the exact state caught in the act:
-- one settled file contributing 0 NiFi lines against 99 SSIS lines.
--
-- The sound test is against the file's TRUE size, and his ledger has it:
-- both engines were handed byte-identical files, so control.stream_file.
-- lines_in is how many lines the file held. NiFi is finished with it when its
-- own accounting adds up to that -- every line either loaded or rejected.
-- Verified across 243 files: 242 accounted for, the 1 exception being the
-- file in flight at the time, which is precisely what should be excluded.
--
-- A file NiFi accounts for differently ON PURPOSE -- the malformed-file case,
-- where an unparseable line fails the whole file as READER_FAILURE -- never
-- settles and so never enters these totals. That is not hidden: v_compare_
-- files is deliberately NOT restricted to settled files, so the file still
-- shows there with both engines' verdicts beside it.
--
-- The SSIS side needs no equivalent: his runner is sequential and writes
-- control.stream_file after the file is fully staged, so the row existing
-- already implies completion.
-- ---------------------------------------------------------------------
CREATE VIEW v_settled_files AS
SELECT j.source_file
  FROM (SELECT source_file, records_valid, records_invalid FROM job_runs
         WHERE source_file IS NOT NULL) j
  JOIN (SELECT source_file, lines_in FROM ssis.stream_file) s
    ON s.source_file = j.source_file
 WHERE j.records_valid + j.records_invalid = s.lines_in;


-- One row per file, both engines' verdicts beside each other. The place to
-- look when a total disagrees: if a file is missing from one side it is in
-- flight, if it is on both with different counts the difference is real.
CREATE VIEW v_compare_files AS
SELECT COALESCE(n.source_file, s.source_file)      AS source_file,
       n.status                                    AS nifi_status,
       n.records_valid                             AS nifi_lines_ok,
       n.records_invalid                           AS nifi_lines_rejected,
       s.status                                    AS ssis_status,
       s.lines_clean                               AS ssis_lines_ok,
       s.lines_rejected                            AS ssis_lines_rejected,
       CASE WHEN n.source_file IS NULL THEN 'not in NiFi yet'
            WHEN s.source_file IS NULL THEN 'not in SSIS yet'
            WHEN COALESCE(n.records_invalid, 0) <> COALESCE(s.lines_rejected, 0)
                 THEN 'rejects differ'
            WHEN COALESCE(n.records_valid, 0) <> COALESCE(s.lines_clean, 0)
                 THEN 'loaded differ'
            ELSE 'agree' END                       AS verdict,
       COALESCE(n.finished_at, s.processed_at)     AS seen_at
  FROM (SELECT source_file, status, records_valid, records_invalid, finished_at
          FROM job_runs) n
  FULL OUTER JOIN ssis.stream_file s ON s.source_file = n.source_file;


-- ---------------------------------------------------------------------
-- The reject log, both engines, one table.
--
-- The SSIS side keeps the offending values in a JSONB payload while this side
-- keeps them as real columns; both are flattened to the same shape here so one
-- table can show either engine, and `engine` can be filtered or grouped on.
-- ---------------------------------------------------------------------
CREATE VIEW v_reject_log AS
SELECT 'NiFi'::text     AS engine,
       q.quarantined_at AS at,
       q.reason,
       q.source_file,
       q.order_id,
       q.line_no,
       q.sku,
       q.qty,
       q.unit_price,
       q.currency,
       q.detail
  FROM quarantine_records q
UNION ALL
SELECT 'SSIS'::text,
       d.detected_at,
       d.error_type,
       d.source_file,
       d.order_id,
       d.raw_payload->>'line_no',
       d.raw_payload->>'sku',
       d.raw_payload->>'qty',
       d.raw_payload->>'unit_price',
       d.raw_payload->>'currency',
       d.error_detail
  FROM ssis.dlq_errors d;


-- Reject counts by reason, both engines, settled files only. A reason either
-- engine has ever produced appears, so an engine-only reason shows as a row
-- with a zero -- which an inner join would have hidden.
CREATE VIEW v_compare_reasons AS
WITH settled AS (SELECT source_file FROM v_settled_files),
     n AS (SELECT reason, count(*) AS n
             FROM quarantine_records
            WHERE source_file IN (SELECT source_file FROM settled)
            GROUP BY 1),
     s AS (SELECT error_type AS reason, count(*) AS n
             FROM ssis.dlq_errors
            WHERE source_file IN (SELECT source_file FROM settled)
            GROUP BY 1)
SELECT COALESCE(n.reason, s.reason)     AS reason,
       COALESCE(n.n, 0)                 AS nifi,
       COALESCE(s.n, 0)                 AS ssis,
       COALESCE(n.n, 0) - COALESCE(s.n, 0) AS delta
  FROM n FULL OUTER JOIN s ON s.reason = n.reason;


-- Headline totals, settled files only. One row, so it drives the stat tiles.
CREATE VIEW v_compare_totals AS
WITH settled AS (SELECT source_file FROM v_settled_files)
SELECT
  (SELECT count(*) FROM v_settled_files)                        AS files_settled,
  (SELECT count(DISTINCT o.order_id) FROM orders o
     JOIN job_runs j ON j.batch_id = o.batch_id
    WHERE j.source_file IN (SELECT source_file FROM settled))   AS nifi_orders,
  (SELECT count(DISTINCT order_id) FROM ssis.clean_sales
    WHERE source_file IN (SELECT source_file FROM settled))     AS ssis_orders,
  (SELECT count(*) FROM order_items i
     JOIN job_runs j ON j.batch_id = i.batch_id
    WHERE j.source_file IN (SELECT source_file FROM settled))   AS nifi_lines,
  (SELECT count(*) FROM ssis.clean_sales
    WHERE source_file IN (SELECT source_file FROM settled))     AS ssis_lines,
  (SELECT count(*) FROM quarantine_records
    WHERE source_file IN (SELECT source_file FROM settled))     AS nifi_rejects,
  (SELECT count(*) FROM ssis.dlq_errors
    WHERE source_file IN (SELECT source_file FROM settled))     AS ssis_rejects,
  (SELECT count(*) FROM alerts a
     JOIN job_runs j ON j.batch_id = a.batch_id
    WHERE j.source_file IN (SELECT source_file FROM settled))   AS nifi_alerts,
  (SELECT count(*) FROM ssis.stream_alerts
    WHERE source_file IN (SELECT source_file FROM settled))     AS ssis_alerts;


-- ---------------------------------------------------------------------
-- The records the two engines actually disagree about.
--
-- Keyed on (source_file, order_id, line_no) and joined FULL OUTER, so every
-- kind of disagreement is one row with its own verdict:
--   rejected only by NiFi / only by SSIS  -- one loaded what the other threw out
--   different reason                      -- both rejected it, for different rules
-- Rows where the engines agree are excluded; an empty result IS the pass.
-- ---------------------------------------------------------------------
CREATE VIEW v_reject_diff AS
WITH settled AS (SELECT source_file FROM v_settled_files),
     n AS (SELECT source_file, order_id, COALESCE(line_no, '') AS line_no,
                  reason, detail
             FROM quarantine_records
            WHERE source_file IN (SELECT source_file FROM settled)),
     s AS (SELECT source_file, order_id,
                  COALESCE(raw_payload->>'line_no', '') AS line_no,
                  error_type AS reason, error_detail AS detail
             FROM ssis.dlq_errors
            WHERE source_file IN (SELECT source_file FROM settled))
SELECT COALESCE(n.source_file, s.source_file) AS source_file,
       COALESCE(n.order_id, s.order_id)       AS order_id,
       COALESCE(n.line_no, s.line_no)         AS line_no,
       n.reason                               AS nifi_reason,
       s.reason                               AS ssis_reason,
       CASE WHEN s.reason IS NULL THEN 'NiFi rejected, SSIS loaded'
            WHEN n.reason IS NULL THEN 'SSIS rejected, NiFi loaded'
            ELSE 'both rejected, different reason' END AS verdict,
       COALESCE(n.detail, s.detail)           AS detail
  FROM n FULL OUTER JOIN s
    ON  s.source_file = n.source_file
    AND s.order_id    = n.order_id
    AND s.line_no     = n.line_no
 WHERE n.reason IS DISTINCT FROM s.reason;


-- A single number for "do the engines agree right now", as a percentage of
-- settled reject decisions. 100 means every settled record got the same
-- verdict from both engines.
CREATE VIEW v_agreement AS
SELECT CASE WHEN total = 0 THEN 100.0
            ELSE round(100.0 * (total - differing) / total, 2) END AS agreement_pct,
       total      AS decisions,
       differing  AS disagreements
  FROM (SELECT (SELECT count(*) FROM v_reject_log r
                 WHERE r.source_file IN (SELECT source_file FROM v_settled_files))
               AS total,
               (SELECT count(*) FROM v_reject_diff) AS differing) t;


-- ---------------------------------------------------------------------
-- The reject log, ONE ROW PER RECORD, both engines' verdicts beside each
-- other.
--
-- v_reject_log above interleaves the two engines, which shows everything but
-- makes the reader pair the rows up by eye: the same order scrolls past twice,
-- once blue and once orange, and telling "both rejected it" from "only one
-- did" means scanning for a partner row that may be several lines away.
--
-- Here the two sides are JOINed on the record key, so one line answers the
-- whole question: what arrived, what NiFi said, what SSIS said, and whether
-- they agree. `agree` is the column to sort on.
--
-- FULL OUTER, so a record only one engine rejected still gets a row -- with a
-- dash opposite. That is the case that matters most and an inner join would
-- drop it.
-- ---------------------------------------------------------------------
CREATE VIEW v_reject_side_by_side AS
WITH settled AS (SELECT source_file FROM v_settled_files),
     n AS (
    SELECT source_file, order_id, COALESCE(line_no, '') AS line_no,
           reason, detail, sku, qty, unit_price, currency, quarantined_at AS at
      FROM quarantine_records
     -- settled files only. Without this the newest rows all read "SSIS only",
     -- because his runner has finished a file NiFi is still working on -- the
     -- in-flight skew, arriving as a wall of fake disagreements at the top of
     -- the table, which is the exact confusion this board exists to remove.
     WHERE source_file IN (SELECT source_file FROM settled)),
     s AS (
    SELECT source_file, order_id,
           COALESCE(raw_payload->>'line_no', '') AS line_no,
           error_type AS reason, error_detail AS detail,
           raw_payload->>'sku'        AS sku,
           raw_payload->>'qty'        AS qty,
           raw_payload->>'unit_price' AS unit_price,
           raw_payload->>'currency'   AS currency,
           detected_at AS at
      FROM ssis.dlq_errors
     WHERE source_file IN (SELECT source_file FROM settled))
SELECT COALESCE(n.at, s.at)                   AS at,
       COALESCE(n.order_id, s.order_id)       AS order_id,
       COALESCE(n.line_no, s.line_no)         AS line_no,
       -- the offending values, taken from whichever engine caught it; both
       -- read the same input line, so either copy is the same record
       COALESCE(n.sku, s.sku)                 AS sku,
       COALESCE(n.qty, s.qty)                 AS qty,
       COALESCE(n.unit_price, s.unit_price)   AS unit_price,
       COALESCE(n.currency, s.currency)       AS currency,
       COALESCE(n.reason, '— loaded —')       AS nifi_says,
       COALESCE(s.reason, '— loaded —')       AS ssis_says,
       CASE WHEN n.reason IS NOT DISTINCT FROM s.reason THEN 'same'
            WHEN n.reason IS NULL THEN 'SSIS only'
            WHEN s.reason IS NULL THEN 'NiFi only'
            ELSE 'different reason' END       AS agree,
       COALESCE(n.detail, s.detail)           AS why,
       COALESCE(n.source_file, s.source_file) AS source_file
  FROM n FULL OUTER JOIN s
    ON  s.source_file = n.source_file
    AND s.order_id    = n.order_id
    AND s.line_no     = n.line_no;
