#!/usr/bin/env bash
# Every rejected record, from BOTH engines, and exactly what they disagree on.
#
# One psql session does all of it. The SSIS warehouse is readable from this one
# through postgres_fdw (db/init/04_ssis_fdw.sql), so this is a JOIN rather than
# two queries the reader has to compare by eye -- which also means it can be
# restricted to files BOTH engines have finished, and an engine that is one
# file behind never looks like a disagreement.
#
# usage: scripts/show_errors.sh [limit]        (limit 0 = skip the log)
set -uo pipefail
N="${1:-40}"

PSQL=(docker compose exec -T postgres psql -U etl -d etldemo -P pager=off)

bold() { printf '\033[1m%s\033[0m\n' "$*"; }

bold "agreement"
"${PSQL[@]}" -c "
  SELECT agreement_pct AS \"agreement %\",
         decisions      AS \"reject decisions\",
         disagreements  AS \"disagreements\",
         (SELECT files_settled FROM v_compare_totals) AS \"files compared\"
    FROM v_agreement;" 2>&1

bold "totals (files both engines have finished)"
"${PSQL[@]}" -c "
  SELECT 'orders'  AS measure, nifi_orders  AS nifi, ssis_orders  AS ssis,
         CASE WHEN nifi_orders  = ssis_orders  THEN 'ok' ELSE 'DIFFERS' END AS v
    FROM v_compare_totals
  UNION ALL SELECT 'lines loaded', nifi_lines, ssis_lines,
         CASE WHEN nifi_lines   = ssis_lines   THEN 'ok' ELSE 'DIFFERS' END
    FROM v_compare_totals
  UNION ALL SELECT 'rejected', nifi_rejects, ssis_rejects,
         CASE WHEN nifi_rejects = ssis_rejects THEN 'ok' ELSE 'DIFFERS' END
    FROM v_compare_totals
  UNION ALL SELECT 'alerts', nifi_alerts, ssis_alerts,
         CASE WHEN nifi_alerts  = ssis_alerts  THEN 'ok' ELSE 'DIFFERS' END
    FROM v_compare_totals;" 2>&1

bold "rejections by reason"
# A FULL OUTER JOIN, so a reason only one engine ever produces still gets a
# row -- with a zero opposite it. That is the case worth catching: it means an
# engine is missing a rule, and an inner join would have hidden it.
"${PSQL[@]}" -c "
  SELECT reason, nifi, ssis, delta,
         CASE WHEN delta = 0 THEN 'ok' ELSE 'DIFFERS' END AS v
    FROM v_compare_reasons ORDER BY reason;" 2>&1

bold "records the engines disagree about"
"${PSQL[@]}" -c "
  SELECT order_id, line_no, verdict,
         COALESCE(nifi_reason, '-') AS nifi,
         COALESCE(ssis_reason, '-') AS ssis,
         left(COALESCE(detail, ''), 44) AS detail
    FROM v_reject_diff ORDER BY order_id DESC LIMIT 30;" 2>&1
echo "  (an empty table here is the pass: every settled record got the same verdict)"

if [ "$N" != "0" ]; then
  echo
  bold "reject log - both engines, newest $N"
  "${PSQL[@]}" -c "
    SELECT to_char(at,'HH24:MI:SS') AS at, engine, reason, order_id, line_no,
           sku, qty, unit_price, currency, left(COALESCE(detail,''), 34) AS detail
      FROM v_reject_log ORDER BY at DESC, engine LIMIT $N;" 2>&1
fi

echo
printf '\033[2mstored in: pgdata volume (NiFi) / etl_pgdata volume (SSIS)\n'
printf 'unreadable FILES are kept as files: data/dlq/\n'
printf 'to share them outside Docker: make errors-export\n'
printf 'if these queries error, the SSIS stack is down - that is the honest answer\033[0m\n'
