#!/usr/bin/env python3
"""
Empty every connection queue in the running flow.

Truncating the warehouse is not enough to start a comparison run clean.
Stopping the flow leaves FlowFiles parked in connections; the next
`start-flow` pushes them straight into the tables you just emptied, and the
two engines end up compared on different input.

NiFi will not let a queue be cleared directly -- it has to be asked via a
drop-request, polled until it finishes, then deleted. That is what this does.

    python3 scripts/drain_queues.py              # drop everything
    python3 scripts/drain_queues.py --count-only # just report queue depth
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8080/nifi-api"


def api(path: str, method: str = "GET") -> dict:
    req = urllib.request.Request(f"{BASE}{path}", method=method)
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read()
    return json.loads(body) if body else {}


def connections() -> list[dict]:
    """Every connection in the root group and its children."""
    found: list[dict] = []
    groups = ["root"]
    while groups:
        gid = groups.pop()
        flow = api(f"/flow/process-groups/{gid}")["processGroupFlow"]["flow"]
        found.extend(flow.get("connections", []))
        groups.extend(g["id"] for g in flow.get("processGroups", []))
    return found


def queued(conns: list[dict]) -> int:
    return sum(c["status"]["aggregateSnapshot"]["flowFilesQueued"] for c in conns)


def drain(conn: dict) -> int:
    """Issue a drop-request and wait for it. Returns flowfiles dropped."""
    cid = conn["id"]
    request = api(f"/flowfile-queues/{cid}/drop-requests", method="POST")
    rid = request["dropRequest"]["id"]
    for _ in range(60):
        state = api(f"/flowfile-queues/{cid}/drop-requests/{rid}")["dropRequest"]
        if state.get("finished"):
            break
        time.sleep(0.5)
    dropped = int((state.get("dropped") or "0 / 0 bytes").split()[0].replace(",", ""))
    api(f"/flowfile-queues/{cid}/drop-requests/{rid}", method="DELETE")
    return dropped


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--count-only", action="store_true",
                    help="print the queue depth and exit")
    args = ap.parse_args()

    try:
        conns = connections()
    except (urllib.error.URLError, OSError) as exc:
        # Not an error worth stopping a run for: the caller may be draining
        # before the stack is up.
        print("0" if args.count_only else f"NiFi not reachable ({exc}) - nothing to drain",
              file=sys.stdout if args.count_only else sys.stderr)
        return 0

    depth = queued(conns)
    if args.count_only:
        print(depth)
        return 0

    if depth == 0:
        print("   queues already empty")
        return 0

    total = sum(drain(c) for c in conns
                if c["status"]["aggregateSnapshot"]["flowFilesQueued"])
    print(f"   dropped {total:,} queued flowfiles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
