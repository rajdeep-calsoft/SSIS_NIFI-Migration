#!/usr/bin/env bash
# Start or stop a NAMED process group on the destination NiFi, and verify it
# actually got there before reporting success.
#
# Generalised from destination/scripts/flow_state.sh (which hardcodes the
# single group "ecommerce_etl") so the same ask-then-verify-then-retry logic
# works for whichever group is being toggled -- the hand-built flow or the
# SSIS2NIFI-generated one. See that file's comment for why a bare PUT is not
# enough: NiFi accepts "start" asynchronously, and a controller service still
# coming up can leave the group silently half-started.
#
# Usage: flow_state.sh GROUP_NAME RUNNING|STOPPED
# If GROUP_NAME does not exist on the canvas, this exits 0 (nothing to do) --
# that is the normal case for "stop the generated flow" before it has ever
# been deployed once.
set -euo pipefail
GROUP="${1:?usage: flow_state.sh GROUP_NAME RUNNING|STOPPED}"
STATE="${2:?usage: flow_state.sh GROUP_NAME RUNNING|STOPPED}"
NIFI="${NIFI_URL:-http://localhost:8080}"

GID=$(curl -sf "$NIFI/nifi-api/flow/process-groups/root" \
  | python3 -c "
import json, sys
g = json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']
hit = next((x['id'] for x in g if x['component']['name'] == '$GROUP'), None)
print(hit or '')")

if [ -z "$GID" ]; then
  echo "group '$GROUP' is not on the canvas -- nothing to $STATE"
  exit 0
fi

ask() {
  curl -sf -X PUT -H 'Content-Type: application/json' \
    -d "{\"id\":\"$GID\",\"state\":\"$STATE\"}" \
    "$NIFI/nifi-api/flow/process-groups/$GID" > /dev/null
}

counts() {
  curl -sf "$NIFI/nifi-api/flow/process-groups/root" | python3 -c "
import json, sys
g = json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']
x = next(i for i in g if i['id'] == '$GID')
print(x.get('runningCount', 0), x.get('stoppedCount', 0), x.get('invalidCount', 0))"
}

for attempt in 1 2 3; do
  ask
  for _ in $(seq 1 15); do
    read -r RUNNING STOPPED INVALID <<< "$(counts)"
    if [ "$STATE" = "RUNNING" ] && [ "${STOPPED:-1}" = "0" ]; then
      echo "group '$GROUP' -> RUNNING ($RUNNING processors)"
      exit 0
    fi
    if [ "$STATE" = "STOPPED" ] && [ "${RUNNING:-1}" = "0" ]; then
      echo "group '$GROUP' -> STOPPED ($STOPPED processors)"
      exit 0
    fi
    sleep 2
  done
  echo "  attempt $attempt: $RUNNING running / $STOPPED stopped / $INVALID invalid - retrying" >&2
done

echo "REFUSING to report success: '$GROUP' is $RUNNING running, $STOPPED stopped, $INVALID invalid" >&2
exit 1
