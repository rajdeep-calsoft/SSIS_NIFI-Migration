#!/usr/bin/env bash
# Both engines, side by side, refreshing. The terminal version of the
# comparison dashboard.
#
# Every number comes from one query against the cross-engine views
# (db/init/04_ssis_fdw.sql), so this shows the same figures the board does --
# restricted to files BOTH engines have finished. That restriction is the
# whole reason this view is trustworthy: without it an engine that is one file
# behind reports a difference every few seconds that resolves itself, and a
# reader cannot tell those from a real one.
set -uo pipefail

INTERVAL="${1:-2}"
PSQL=(docker compose exec -T postgres psql -U etl -d etldemo -tAF'|')

while true; do
  totals=$("${PSQL[@]}" -c "
    SELECT files_settled, nifi_orders, ssis_orders, nifi_lines, ssis_lines,
           nifi_rejects, ssis_rejects, nifi_alerts, ssis_alerts,
           (SELECT agreement_pct FROM v_agreement),
           (SELECT disagreements FROM v_agreement),
           (SELECT count(*) FROM v_compare_files WHERE verdict LIKE 'not in %')
      FROM v_compare_totals;" 2>/dev/null)

  clear
  if [ -z "$totals" ]; then
    printf '\033[1m  NiFi  vs  SSIS \033[0m\n\n'
    printf '  \033[33mcannot read the comparison views.\033[0m\n\n'
    printf '  Usually the SSIS stack is down:\n'
    printf '    docker compose -f ../source/docker-compose.yml up -d\n'
    printf '  If it moved, re-point the foreign server:  make fdw\n'
    sleep "$INTERVAL"; continue
  fi

  IFS='|' read -r files n_ord s_ord n_lin s_lin n_rej s_rej n_alr s_alr \
                  agree diffs inflight <<<"$totals"

  printf '\033[1m  NiFi  vs  SSIS \033[0m  -  one stream, both engines   (%s)\n\n' \
         "$(date +%H:%M:%S)"

  if [ "${diffs:-0}" = "0" ]; then
    printf '  agreement \033[32m%s%%\033[0m   files compared %s   in flight %s\n\n' \
           "$agree" "$files" "$inflight"
  else
    printf '  agreement \033[33m%s%%\033[0m   \033[33m%s disagreement(s)\033[0m   files compared %s   in flight %s\n\n' \
           "$agree" "$diffs" "$files" "$inflight"
  fi

  printf '  %-22s %10s %10s\n' "" "NiFi" "SSIS"
  printf '  %s\n' "----------------------------------------------------------"

  row() {
    local label="$1" a="$2" b="$3" mark
    if [ "$a" = "$b" ]; then mark=$'\033[32m  ok\033[0m'
    else mark=$'\033[33m  differs\033[0m'; fi
    printf '  %-22s %10s %10s %b\n' "$label" "$a" "$b" "$mark"
  }
  row "orders"           "$n_ord" "$s_ord"
  row "order lines"      "$n_lin" "$s_lin"
  row "rejected records" "$n_rej" "$s_rej"
  row "alerts"           "$n_alr" "$s_alr"

  echo
  printf '\033[1m  rejections by reason\033[0m\n\n'
  "${PSQL[@]}" -c "SELECT reason, nifi, ssis, delta FROM v_compare_reasons
                    ORDER BY reason;" 2>/dev/null |
  awk -F'|' '{
      # %s, not %b: awk has already turned \033 into a real ESC when it
      # parsed the string, and %b is a gawk extension that mawk aborts on --
      # which killed the loop after the first row.
      mark = ($4 == 0) ? "\033[32mok\033[0m" : "\033[33mdiffers\033[0m"
      printf "    %-22s %10s %10s   %s\n", $1, $2, $3, mark
    }
    END { if (NR == 0) printf "    (no rejects yet)\n" }'

  if [ "${diffs:-0}" != "0" ]; then
    echo
    printf '\033[1m  what they disagree about\033[0m\n\n'
    "${PSQL[@]}" -c "SELECT order_id, line_no, verdict,
                            COALESCE(nifi_reason,'-'), COALESCE(ssis_reason,'-')
                       FROM v_reject_diff ORDER BY order_id DESC LIMIT 6;" \
      2>/dev/null |
    awk -F'|' '{ printf "    %-26s line %-6s %-28s %s / %s\n", $1, $2, $3, $4, $5 }'
  fi

  echo
  printf '\033[2m  ctrl-c to stop   |   full detail: make errors   |   captured verdict: make compare\033[0m\n'
  sleep "$INTERVAL"
done
