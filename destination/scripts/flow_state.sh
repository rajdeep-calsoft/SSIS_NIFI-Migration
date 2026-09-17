#!/usr/bin/env bash
# Start or stop the ecommerce_etl process group via the NiFi REST API.
#
# The PUT is fire-and-forget: NiFi accepts "start the group" and does it
# asynchronously, processor by processor. A processor whose controller service
# is still coming up just stays stopped, and nothing says so.
#
# That is not theoretical. `8c. Check replay` came up stopped once and the
# pipeline stalled behind it for twenty minutes -- records queued (nothing was
# lost, that is the design) but nothing loaded, while the reject path kept
# working, so the dashboards showed rejects climbing against flat orders. The
# old version of this script printed "flow ecommerce_etl -> RUNNING" and
# exited 0 the whole time.
#
# So: after asking, verify, and retry the ask before giving up.
set -euo pipefail
STATE="${1:?usage: flow_state.sh RUNNING|STOPPED}"
NIFI="${NIFI_URL:-http://localhost:8080}"

GID=$(curl -sf "$NIFI/nifi-api/flow/process-groups/root" \
  | python3 -c "import json,sys; g=json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']; print(next(x['id'] for x in g if x['component']['name']=='ecommerce_etl'))")

# "Exactly one flow live on destination NiFi" (this file's own CLAUDE.md) is
# only true if whatever starts ecommerce_etl also stops anything else that's
# RUNNING first. Without this, `make bulk-run` -> flow_state.sh RUNNING
# starts ecommerce_etl on top of an already-running deployed-via-api flow
# (e.g. ssis2nifi-pkg_orders_etl) and both double-write the same tables --
# found the hard way, api/app.py's _stop_other_running_groups already does
# this for the api's own /deploy and /flow/use-* endpoints; this is the same
# rule enforced for this script's own callers (make bulk-run, make start-flow).
stop_other_running_groups() {
  local others
  others=$(curl -sf "$NIFI/nifi-api/flow/process-groups/root" | python3 -c "
import json,sys
g=json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']
for x in g:
    if x['component']['name'] != 'ecommerce_etl' and x.get('runningCount', 0) > 0:
        print(x['id'], x['component']['name'])
")
  [ -n "$others" ] || return 0
  while read -r other_id other_name; do
    echo "  stopping '$other_name' first -- only one flow may run at a time"
    curl -sf -X PUT -H 'Content-Type: application/json' \
      -d "{\"id\":\"$other_id\",\"state\":\"STOPPED\"}" \
      "$NIFI/nifi-api/flow/process-groups/$other_id" > /dev/null
    for _ in $(seq 1 15); do
      running=$(curl -sf "$NIFI/nifi-api/flow/process-groups/root" | python3 -c "
import json,sys
g=json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']
x=next((i for i in g if i['id']=='$other_id'), None)
print(x.get('runningCount',0) if x else 0)")
      [ "$running" = "0" ] && break
      sleep 2
    done
  done <<< "$others"
}

ask() {
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"id\":\"$GID\",\"state\":\"$STATE\"}" \
    "$NIFI/nifi-api/flow/process-groups/$GID" > /dev/null
}

# counts live on the process-group ENTITY, not inside its status snapshot
counts() {
  curl -sf "$NIFI/nifi-api/flow/process-groups/root" | python3 -c "
import json,sys
g=json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']
x=next(i for i in g if i['component']['name']=='ecommerce_etl')
print(x.get('runningCount',0), x.get('stoppedCount',0), x.get('invalidCount',0))"
}

want_stopped_zero=1
[ "$STATE" = "STOPPED" ] && want_stopped_zero=0

[ "$STATE" = "RUNNING" ] && stop_other_running_groups

for attempt in 1 2 3; do
  ask
  for _ in $(seq 1 15); do
    read -r RUNNING STOPPED INVALID <<< "$(counts)"
    if [ "$STATE" = "RUNNING" ] && [ "${STOPPED:-1}" = "0" ]; then
      echo "flow ecommerce_etl -> RUNNING ($RUNNING processors)"
      exit 0
    fi
    if [ "$STATE" = "STOPPED" ] && [ "${RUNNING:-1}" = "0" ]; then
      echo "flow ecommerce_etl -> STOPPED ($STOPPED processors)"
      exit 0
    fi
    sleep 2
  done
  echo "  attempt $attempt: $RUNNING running / $STOPPED stopped / $INVALID invalid - retrying" >&2
done

echo "REFUSING to report success: flow is $RUNNING running, $STOPPED stopped, $INVALID invalid" >&2
[ "${INVALID:-0}" != "0" ] && echo "  invalid processors cannot start - check the canvas at $NIFI/nifi" >&2
exit 1
