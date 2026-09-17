#!/usr/bin/env bash
# Wipe every process group off the destination NiFi canvas -- for starting a
# demo fresh, not a normal part of the toggle flow (see use-generated.sh /
# use-handbuilt.sh for that).
#
# A group can't be deleted while it's RUNNING, or while it (or any NESTED
# child group) has an ENABLED controller service -- and some services can't
# disable while another enabled one still references them (e.g. a
# DBCPConnectionPool referenced by several lookup services). So this stops
# each top-level group, walks its whole descendant tree (nested children
# are not listed by the root's own process-groups call -- found the hard
# way), disables every service it finds in retry passes, then deletes the
# top-level group (deletion cascades to its nested children once nothing
# blocks it).
set -euo pipefail
NIFI="${NIFI_URL:-http://localhost:8080}"

python3 -c "
import json, time, urllib.request

BASE = '$NIFI/nifi-api'

def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                                headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())

def stop_group(pg_id):
    for _ in range(15):
        req('PUT', f'/flow/process-groups/{pg_id}', {'id': pg_id, 'state': 'STOPPED'})
        status = req('GET', f'/process-groups/{pg_id}')['status']['aggregateSnapshot']
        if status.get('runningCount', 0) == 0 and status.get('activeThreadCount', 0) == 0:
            return True
        time.sleep(2)
    return False

def descendants(pg_id):
    ids = []
    for child in req('GET', f'/process-groups/{pg_id}/process-groups')['processGroups']:
        cid = child['component']['id']
        ids.append(cid)
        ids.extend(descendants(cid))
    return ids

def disable_all_services(pg_id):
    for _ in range(6):
        svcs = req('GET', f'/flow/process-groups/{pg_id}/controller-services')['controllerServices']
        enabled = [s for s in svcs if s['component']['state'] == 'ENABLED']
        if not enabled:
            return True
        progress = False
        for s in enabled:
            c = s['component']
            try:
                req('PUT', f'/controller-services/{c[\"id\"]}/run-status',
                    {'revision': {'version': s['revision']['version']}, 'state': 'DISABLED'})
                progress = True
            except Exception:
                pass
        time.sleep(1)
        if not progress:
            return False
    return False

top = req('GET', '/process-groups/root/process-groups')['processGroups']
if not top:
    print('canvas already empty -- nothing to clear')
else:
    for pg in top:
        pg_id, name = pg['component']['id'], pg['component']['name']
        stop_group(pg_id)
        for gid in [pg_id] + descendants(pg_id):
            disable_all_services(gid)
        ver = req('GET', f'/process-groups/{pg_id}')['revision']['version']
        req('DELETE', f'/process-groups/{pg_id}?version={ver}')
        print(f'deleted {name}')
    remaining = req('GET', '/process-groups/root/process-groups')['processGroups']
    print('canvas clear' if not remaining else f'WARNING: {len(remaining)} group(s) still remain')
"
