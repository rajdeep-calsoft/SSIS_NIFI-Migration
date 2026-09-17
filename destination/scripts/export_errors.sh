#!/usr/bin/env bash
# Dump both engines' rejects out of Docker and onto the host, as CSV and JSON.
#
# The rejects live in Postgres, inside a Docker volume. That is durable but not
# shareable -- `make reset` destroys it, and nobody can open it without the
# stack running. This writes a copy you can mail, attach to a ticket, or open
# in a spreadsheet.
#
# The CSV columns are identical for both engines on purpose, so the two files
# diff directly.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/data/exports/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"

NIFI_SQL="
  SELECT quarantined_at, reason, order_id, line_no, sku, qty, unit_price,
         currency, detail
    FROM quarantine_records
   ORDER BY quarantined_at, id"

SSIS_SQL="
  SELECT detected_at AS quarantined_at, error_type AS reason, order_id,
         raw_payload->>'line_no'    AS line_no,
         raw_payload->>'sku'        AS sku,
         raw_payload->>'qty'        AS qty,
         raw_payload->>'unit_price' AS unit_price,
         raw_payload->>'currency'   AS currency,
         error_detail               AS detail
    FROM control.dlq_errors
   ORDER BY detected_at, dlq_id"

docker compose exec -T postgres psql -U etl -d etldemo \
  -c "COPY ($NIFI_SQL) TO STDOUT WITH CSV HEADER" > "$OUT/nifi-rejects.csv"
docker exec ssis-warehouse psql -U etl_user -d etl_db \
  -c "COPY ($SSIS_SQL) TO STDOUT WITH CSV HEADER" > "$OUT/ssis-rejects.csv"

# The histograms as JSON, which is what a reviewer actually reads first.
docker compose exec -T postgres psql -U etl -d etldemo -tAc \
  "SELECT coalesce(json_object_agg(reason, n), '{}')::text
     FROM (SELECT reason, count(*) n FROM quarantine_records GROUP BY 1) q" \
  > "$OUT/nifi-by-reason.json"
docker exec ssis-warehouse psql -U etl_user -d etl_db -tAc \
  "SELECT coalesce(json_object_agg(error_type, n), '{}')::text
     FROM (SELECT error_type, count(*) n FROM control.dlq_errors GROUP BY 1) q" \
  > "$OUT/ssis-by-reason.json"

# The files NiFi could not read at all are evidence too, and they are already
# plain files -- copy them rather than describe them.
if compgen -G "$ROOT/data/dlq/*" > /dev/null; then
  mkdir -p "$OUT/unreadable-files"
  cp "$ROOT"/data/dlq/* "$OUT/unreadable-files/" 2>/dev/null || true
fi

echo "exported to $OUT"
for f in "$OUT"/*; do
  [ -f "$f" ] && printf '  %-22s %s\n' "$(basename "$f")" "$(wc -l < "$f") lines"
done
echo
echo "diff the two directly:"
echo "  diff <(cut -d, -f2,3,4 $OUT/nifi-rejects.csv | sort) \\"
echo "       <(cut -d, -f2,3,4 $OUT/ssis-rejects.csv | sort)"
