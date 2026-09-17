#!/usr/bin/env bash
# Put the SSIS2NIFI-GENERATED flow live on the destination NiFi, and make
# sure the hand-built one is not also running -- both write the same
# warehouse tables (orders, order_items, quarantine_records, job_runs), so
# exactly one may be RUNNING at a time.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$HERE")"
PACKAGE="${PACKAGE:-pkg_orders_etl.dtsx}"

if ! curl -sf http://localhost:8088/health >/dev/null 2>&1; then
  echo "api is not up -- starting it (make api-up)" >&2
  make -C "$ROOT" api-up
fi

echo "1/2  stopping the hand-built flow (ecommerce_etl), if it's running..."
"$HERE/flow_state.sh" ecommerce_etl STOPPED

echo "2/2  deploying the generated flow via the curl front door..."
RESPONSE=$(curl -sX POST http://localhost:8088/deploy \
  -H 'content-type: application/json' \
  -d "{\"package\":\"$PACKAGE\"}")
echo "$RESPONSE" | python3 -m json.tool

EXIT_CODE=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('exit_code','?'))" 2>/dev/null || echo "?")
if [ "$EXIT_CODE" != "0" ]; then
  echo "deploy did not succeed (exit_code=$EXIT_CODE) -- see the response above" >&2
  exit 1
fi

echo
"$HERE/which-flow.sh"
