#!/usr/bin/env bash
# Restore the hand-built flow (NIFI-FLOW's ecommerce_etl) as the live one on
# the destination NiFi, stopping the SSIS2NIFI-generated flow first -- both
# write the same warehouse tables, so exactly one may be RUNNING at a time.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$HERE")"
GENERATED_GROUP="${GENERATED_GROUP:-ssis2nifi-pkg_orders_etl}"

echo "1/2  stopping the generated flow ($GENERATED_GROUP), if it's running..."
"$HERE/flow_state.sh" "$GENERATED_GROUP" STOPPED

echo "2/2  starting the hand-built flow (ecommerce_etl)..."
make -C "$ROOT/destination" start-flow

echo
"$HERE/which-flow.sh"
