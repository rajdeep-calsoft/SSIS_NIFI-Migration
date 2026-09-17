#!/usr/bin/env bash
# Push ONE file of your own through BOTH engines and print the verdict.
#
#   scripts/feed_file.sh mydata.json
#   scripts/feed_file.sh mydata.json --keep-generator
#
# For a file you made yourself, or one of the frozen fixtures, or anything a
# colleague hands you. The continuous generator is stopped first so nothing
# else lands while you are watching -- pass --keep-generator to leave it on.
#
# Three things this handles that copying by hand does not:
#
#   * The SSIS engine keys `control.stream_file.source_file` UNIQUE and skips
#     any filename it has already seen. Feeding the same file twice under the
#     same name looks like "his engine ignored it". A timestamp is added to the
#     delivered name so every feed is a new file to both engines.
#   * Both copies are checksummed after writing. A silently unwritable second
#     inbox would otherwise show up as "SSIS is behind", much later.
#   * It waits for BOTH engines to finish before reporting. NiFi picks a file
#     up in about a second, his runner polls every five, so reading the result
#     immediately gives you the in-flight skew instead of the answer.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSIS_REPO="${SSIS_REPO:-$ROOT/../source}"
SHARED="${SHARED_DATA:-$SSIS_REPO/data}"
cd "$ROOT"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
dim()  { printf '\033[2m%s\033[0m\n' "$*"; }

SRC="${1:-}"
KEEP_GEN=0
[ "${2:-}" = "--keep-generator" ] && KEEP_GEN=1

if [ -z "$SRC" ] || [ ! -f "$SRC" ]; then
  cat >&2 <<EOF
usage: scripts/feed_file.sh <file.json> [--keep-generator]

  <file.json>  NDJSON - one JSON record per line. Examples:
                 spec/fixtures/tier1-smoke.ndjson
                 ~/Downloads/whatever-they-sent-me.json
EOF
  exit 1
fi

LINES=$(wc -l < "$SRC")
NAME="feed_$(date -u +%Y%m%dT%H%M%S)_$(basename "${SRC%.*}" | tr -c 'A-Za-z0-9_-' '_').json"

bold "1. one source of data only"
if [ "$KEEP_GEN" = "0" ]; then
  docker compose stop generator >/dev/null 2>&1 || true
  dim "   generator stopped - restart later with: docker compose start generator"
else
  dim "   --keep-generator: the live stream keeps running alongside your file"
fi

bold "2. delivering $(basename "$SRC") ($LINES lines) to both inboxes"
mkdir -p "$ROOT/data/landing" "$SHARED/landing"
cp "$SRC" "$ROOT/data/landing/$NAME"
cp "$SRC" "$SHARED/landing/$NAME"

a=$(sha256sum < "$ROOT/data/landing/$NAME" | cut -d' ' -f1)
b=$(sha256sum < "$SHARED/landing/$NAME"    | cut -d' ' -f1)
[ "$a" = "$b" ] || { echo "REFUSING: the two copies differ ($a vs $b)"; exit 1; }
dim "   $NAME"
dim "   sha256 ${a:0:16}... identical in both inboxes"

bold "3. waiting for both engines to finish it"
for _ in $(seq 1 60); do
  settled=$(docker compose exec -T postgres psql -U etl -d etldemo -tAc \
    "select count(*) from v_settled_files where source_file = '$NAME';" \
    2>/dev/null | tr -d ' ')
  [ "${settled:-0}" = "1" ] && break
  sleep 2
done
[ "${settled:-0}" = "1" ] || dim "   (timed out - one engine has not finished; results below may be partial)"

echo
bold "what each engine did with it"
docker compose exec -T postgres psql -U etl -d etldemo -P pager=off -c "
  SELECT nifi_status, ssis_status,
         nifi_lines_ok      AS nifi_loaded,  ssis_lines_ok      AS ssis_loaded,
         nifi_lines_rejected AS nifi_rejected, ssis_lines_rejected AS ssis_rejected,
         verdict
    FROM v_compare_files WHERE source_file = '$NAME';" 2>&1

bold "every record it rejected, both engines"
docker compose exec -T postgres psql -U etl -d etldemo -P pager=off -c "
  SELECT order_id, line_no, sku, qty, unit_price, currency,
         nifi_says, ssis_says, agree
    FROM v_reject_side_by_side
   WHERE source_file = '$NAME'
   ORDER BY agree, order_id;" 2>&1

echo
# The verdict has to come from v_compare_files, NOT from counting v_reject_diff.
#
# Every "diff" view is restricted to SETTLED files -- files where NiFi's own
# accounting adds up to the line count his engine read -- because that is what
# stops an in-flight file reading as a disagreement. A file NiFi could not
# PARSE never settles: records_valid + records_invalid stays 0 against his
# lines_in of 20, so it is excluded from v_reject_diff, the count comes back
# zero, and the old version of this script printed "the two engines agreed on
# every record" directly underneath a table whose verdict column said
# "rejects differ". Both numbers were right; the sentence was not.
verdict=$(docker compose exec -T postgres psql -U etl -d etldemo -tAc \
     "select verdict from v_compare_files where source_file = '$NAME';" 2>/dev/null | tr -d ' ')
settled=$(docker compose exec -T postgres psql -U etl -d etldemo -tAc \
     "select count(*) from v_settled_files where source_file = '$NAME';" 2>/dev/null | tr -d ' ')
n=$(docker compose exec -T postgres psql -U etl -d etldemo -tAc \
     "select count(*) from v_reject_diff where source_file = '$NAME';" 2>/dev/null | tr -d ' ')
readerfail=$(docker compose exec -T postgres psql -U etl -d etldemo -tAc \
     "select count(*) from quarantine_records
       where source_file = '$NAME' and reason = 'READER_FAILURE';" 2>/dev/null | tr -d ' ')

if [ "${readerfail:-0}" != "0" ]; then
  printf '\033[33mNiFi could not PARSE this file, so it failed the whole file.\033[0m\n'
  cat <<'NOTE'
  This is known difference #2, not a new bug -- see spec/CONFORMANCE.md.
    NiFi  one READER_FAILURE row, the file parked in data/dlq/, job FAILED
    SSIS  each unreadable LINE rejected on its own as PARSE_ERROR, file PASS

  Because NiFi loaded and rejected nothing, its accounting never adds up to
  the line count, so the file never becomes "settled" and is invisible to the
  record-level comparison. That is why the per-record table above is empty --
  there are no NiFi records to line his up against.

  Feed malformed lines as their own small file, never mixed into a good batch.
NOTE
elif [ "${verdict}" = "agree" ]; then
  printf '\033[32mthe two engines agreed on every record in this file.\033[0m\n'
elif [ "${settled:-0}" = "0" ]; then
  printf '\033[33mone engine has not finished this file, so there is no verdict yet.\033[0m\n'
  printf '  re-run:  make watch-both     (wait for "Still in flight" to reach 0)\n'
else
  printf '\033[33mverdict: %s -- %s record(s) handled differently.\033[0m\n' "$verdict" "$n"
  printf '  the `agree` column above names them.\n'
fi
[ "$KEEP_GEN" = "0" ] && dim "restart the live stream with: docker compose start generator"
exit 0
