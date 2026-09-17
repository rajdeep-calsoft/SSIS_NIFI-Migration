#!/usr/bin/env bash
# Run one fixture through BOTH engines from a guaranteed-clean start.
#
# Every step here exists because skipping it produced a wrong answer once:
#
#   * The streaming generators must be DOWN. Left running, each engine gets its
#     own live batches on top of the fixture and the comparison is meaningless.
#     (A first attempt at this compared 1,544 NiFi orders against 97 SSIS ones
#     for exactly that reason.)
#   * NiFi's queues must be drained, not just its tables truncated. Stopping the
#     flow leaves FlowFiles parked in connections; restarting it flushes them
#     into a warehouse you thought was empty.
#   * The SSIS side keeps `control.stream_file.source_file` as a UNIQUE
#     watermark and builds its dedupe set from ALL of `stage.clean_sales`, so
#     leftovers there turn every replayed line into a DUP_KEY.
#
# usage: scripts/parallel_run.sh <tier>
set -euo pipefail

TIER="${1:-tier1-smoke}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSIS_REPO="${SSIS_REPO:-$ROOT/../source}"
cd "$ROOT"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
dim()  { printf '\033[2m%s\033[0m\n' "$*"; }

# Whichever group is live right now stays the one under test -- this script
# used to hardcode ecommerce_etl (via the local flow_state.sh), so running it
# while a package deployed through api/ was live silently switched the canvas
# over to the hand-built flow instead. Captured once, up front, before
# anything below stops or restarts it.
NIFI="${NIFI_URL:-http://localhost:8080}"
LIVE_GROUP=$(curl -sf "$NIFI/nifi-api/flow/process-groups/root" 2>/dev/null | python3 -c "
import json, sys
try:
    g = json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']
except Exception:
    g = []
running = [x['component']['name'] for x in g if x.get('runningCount', 0) > 0]
print(running[0] if len(running) == 1 else '')
" 2>/dev/null || true)
LIVE_GROUP="${LIVE_GROUP:-ecommerce_etl}"
bold "0. running against: $LIVE_GROUP"

bold "1. stopping both stream generators"
docker compose stop generator >/dev/null 2>&1 || true
docker compose -f "$SSIS_REPO/docker-compose.yml" stop orders_gen >/dev/null 2>&1 || true
# Also stop the SSIS side's own continuous consumer daemon (stream_runner /
# container ssis-engine). Left running, it independently drains the landing
# directory on its own poll loop at the same time step 6 below explicitly
# runs a one-shot orchestrate.py pass over the same directory -- found by
# running this for real: the two raced, `dw.fact_sales` ended up with two
# distinct etl_batch_id values and roughly double the expected row count,
# `staged` and `clean+rejected` disagreed, and the batch line printed FAIL.
# Step 6's orchestrate.py call is this comparison's own one-shot equivalent
# of NiFi's "restart the flow" in step 4 -- the daemon must stay stopped
# for the same reason the generator does.
docker compose -f "$SSIS_REPO/docker-compose.yml" stop stream_runner >/dev/null 2>&1 || true
dim "   nothing may write to either landing directory but this script"

bold "2. draining the NiFi flow"
bash ../scripts/flow_state.sh "$LIVE_GROUP" STOPPED >/dev/null 2>&1 || true
sleep 3
python3 scripts/drain_queues.py
rm -f data/landing/*.json data/archive/*.json data/dlq/*.json 2>/dev/null || true

bold "3. clearing both warehouses (dimensions kept)"
docker compose exec -T postgres psql -U etl -d etldemo -q -c \
  "TRUNCATE orders, order_items, alerts, quarantine_records, job_runs;"
docker exec ssis-toolbox python3 /app/scripts/apply_sql.py \
  /app/scripts/reset_stream.sql >/dev/null
rm -f "$ROOT/../source/data/landing"/*.json 2>/dev/null || true

for engine in nifi ssis; do
  case $engine in
    nifi) n=$(docker compose exec -T postgres psql -U etl -d etldemo -tAc \
            "select (select count(*) from orders)+(select count(*) from quarantine_records);") ;;
    ssis) n=$(docker exec ssis-warehouse psql -U etl_user -d etl_db -tAc \
            "select (select count(*) from dw.fact_sales)+(select count(*) from control.dlq_errors);") ;;
  esac
  [ "${n// /}" = "0" ] || { echo "REFUSING: $engine warehouse is not empty ($n rows)"; exit 1; }
  dim "   $engine: empty"
done

bold "4. restarting the NiFi flow"
bash ../scripts/flow_state.sh "$LIVE_GROUP" RUNNING >/dev/null 2>&1 || true
sleep 3

bold "5. injecting $TIER into both"
python3 spec/fixtures/inject_fixture.py "$TIER"

bold "6. running the SSIS pipeline"
docker exec ssis-toolbox python3 /app/scripts/orchestrate.py \
  --skip-gen --skip-tests 2>&1 | grep -E '^\s*\[batch|^\[4/6\]|totals:' || true

bold "7. waiting for NiFi to drain"
for _ in $(seq 1 60); do
  queued=$(python3 scripts/drain_queues.py --count-only 2>/dev/null || echo 0)
  [ "$queued" = "0" ] && sleep 4 && break
  sleep 2
done

echo
bold "both engines have processed $TIER. Now:"
echo "  make capture ENGINE=nifi && make capture ENGINE=ssis && make compare"
