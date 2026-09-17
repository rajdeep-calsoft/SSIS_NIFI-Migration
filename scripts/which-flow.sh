#!/usr/bin/env bash
# Which process group is live on the destination NiFi right now -- the
# hand-built one, the generated one, both (a bug -- they write the same
# tables) or neither.
set -euo pipefail
NIFI="${NIFI_URL:-http://localhost:8080}"

curl -sf "$NIFI/nifi-api/flow/process-groups/root" | python3 -c "
import json, sys
groups = json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']
if not groups:
    print('no process group on the canvas -- run make use-generated or make use-handbuilt')
    sys.exit(0)
running = []
for g in groups:
    c = g['component']
    state = 'RUNNING' if g.get('stoppedCount', 0) == 0 and g.get('runningCount', 0) else \
            ('STOPPED' if g.get('runningCount', 0) == 0 else 'MIXED')
    print(f\"  {c['name']:<32} {state:<8} \"
          f\"({g.get('runningCount',0)} running / {g.get('stoppedCount',0)} stopped / \"
          f\"{g.get('invalidCount',0)} invalid)\")
    if state == 'RUNNING':
        running.append(c['name'])
print()
if len(running) > 1:
    print(f'WARNING: more than one group is RUNNING at once: {running}')
    print('         both write the same warehouse tables -- stop one.')
elif len(running) == 1:
    print(f'live: {running[0]}')
else:
    print('nothing is RUNNING')
"
