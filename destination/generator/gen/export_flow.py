"""Dump the live process group back to nifi/flow/ecommerce_etl.flow.json.

Run this after hand-editing the canvas (`make export-flow`) so the committed
artifact matches what you just built by clicking. Sensitive properties come
back null by design -- provision.py re-injects the DB password on import.
"""
from __future__ import annotations

import json
import os

from .build_flow import GROUP_NAME
from .nifi_api import NiFi


def run() -> int:
    nifi = NiFi()
    nifi.wait_until_ready()

    root = nifi.root_id()
    group = nifi.find_child_group(root, GROUP_NAME)
    if not group:
        print(f"[export] no process group named '{GROUP_NAME}' on the canvas")
        return 1

    definition = nifi.download_flow_definition(group["id"])
    out = os.getenv("FLOW_FILE", "/flow/ecommerce_etl.flow.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(definition, fh, indent=2)

    size = os.path.getsize(out)
    print(f"[export] wrote {out} ({size:,} bytes)")
    return 0
