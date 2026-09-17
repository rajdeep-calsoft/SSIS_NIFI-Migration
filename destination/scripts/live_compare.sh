#!/usr/bin/env bash
# Start a LIVE side-by-side: one generator, two engines, continuously.
#
# The difference from parallel_run.sh: that one feeds a frozen fixture once and
# stops, which is the authoritative test. This one leaves both engines running
# on a shared stream, which is what you want on a screen during a demo.
#
# The single idea that makes it work: the generator writes each batch to BOTH
# inboxes under the SAME filename (LANDING_DIRS in docker-compose.yml). Neither
# engine generates its own data -- his orders_gen is behind a compose profile
# and stays down, so there is exactly one source of truth for what arrived.
#
# Everything else here is about starting from a state where the two engines
# CAN agree. Skipping any of it produces a permanent, unexplainable gap:
#
#   * NiFi's queues must be drained, not just its tables truncated. A stopped
#     flow parks FlowFiles in the connections; restarting flushes them into a
#     warehouse you believe is empty.
#   * The SSIS side keys `control.stream_file.source_file` UNIQUE and builds
#     its dedupe set from ALL of `stage.clean_sales`, so leftovers there turn
#     replayed lines into DUP_KEY.
#   * Both landing dirs must be empty, or one engine starts with a backlog.
#
# usage: scripts/live_compare.sh [--keep] ; --keep skips the wipe
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSIS_REPO="${SSIS_REPO:-$ROOT/../source}"
SHARED="${SHARED_DATA:-$ROOT/../source/data}"
cd "$ROOT"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
dim()  { printf '\033[2m%s\033[0m\n' "$*"; }

KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

bold "1. one generator only"
docker compose stop generator >/dev/null 2>&1 || true
docker compose -f "$SSIS_REPO/docker-compose.yml" stop orders_gen >/dev/null 2>&1 || true
dim "   his orders_gen stays down: this repo's generator feeds both inboxes"

# His stream_runner has to come down too, not just the generators. It polls the
# shared landing dir every 5s, so if it is left up it re-ingests the leftover
# files the moment step 3 truncates -- the warehouse refills faster than it is
# cleared and step 3's emptiness guard fails with a row count that CHANGES
# between two reads ("not empty (128 rows)", then 283). That looks like a
# broken reset script; it is a race. Step 5 starts it again, after the wipe.
docker compose -f "$SSIS_REPO/docker-compose.yml" stop stream_runner >/dev/null 2>&1 || true
dim "   ssis-engine stopped for the wipe; step 5 brings it back"

if [ "$KEEP" = "0" ]; then
  bold "2. draining the NiFi flow"
  bash scripts/flow_state.sh STOPPED >/dev/null 2>&1 || true
  sleep 3
  python3 scripts/drain_queues.py
  rm -f data/landing/*.json data/archive/*.json data/dlq/*.json 2>/dev/null || true

  bold "3. clearing both warehouses (the shared catalogue is kept)"
  docker compose exec -T postgres psql -U etl -d etldemo -q -c \
    "TRUNCATE orders, order_items, alerts, quarantine_records, job_runs;"
  docker exec ssis-toolbox python3 /app/scripts/apply_sql.py \
    /app/scripts/reset_stream.sql >/dev/null
  rm -f "$SHARED/landing"/*.json 2>/dev/null || true

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
  bash scripts/flow_state.sh RUNNING >/dev/null 2>&1 || true
  sleep 3
else
  bold "2-4. --keep: leaving both warehouses as they are"
fi

bold "5. starting the SSIS engine's daemon"
docker compose -f "$SSIS_REPO/docker-compose.yml" up -d stream_runner >/dev/null 2>&1
dim "   ssis-engine: polls $SHARED/landing every 5s"

bold "6. starting the shared generator"
docker compose up -d generator >/dev/null 2>&1
sleep 6
docker compose logs generator --tail 4 --no-log-prefix 2>/dev/null || true

echo
bold "live. both engines are now eating the same batches."
cat <<EOF
  watch it      http://localhost:3000  ->  "NiFi vs SSIS - same data, same answer?"
  his board     http://localhost:3001  ->  "1 - Pipeline Health (SSIS)"
  in a terminal make watch-both
  the verdict   make capture ENGINE=nifi && make capture ENGINE=ssis && make compare

Note: the SSIS runner polls every 5s and NiFi picks up files within ~1s, so
his numbers trail by a few seconds. They converge; a gap that does NOT close
is a real difference.
EOF
