"""Startup provisioning: get the committed flow onto the canvas and running.

Order matters here:
  1. wait for NiFi's flow controller to be addressable
  2. import the flow definition (skip if the group is already there, so a
     restart does not stack duplicate copies on the canvas)
  3. RE-SET THE DB PASSWORD -- NiFi exports sensitive properties as null, so a
     freshly imported DBCPConnectionPool has no password and will refuse to
     enable. This is the single most likely reason a rebuilt demo sits idle.
  4. enable controller services, then start every processor in the group
"""
from __future__ import annotations

import os

from .build_flow import GROUP_NAME
from .nifi_api import NiFi, NiFiError


def wire_sensitive_properties(nifi: NiFi, gid: str) -> None:
    """Put the DB password back into any connection pool that lost it.

    A controller service can only be reconfigured while DISABLED, and the pool
    cannot be disabled while the lookup services referencing it are still
    enabled -- so the caller must have disabled the whole group first.
    """
    services = nifi.get(f"/flow/process-groups/{gid}/controller-services")
    password = os.getenv("PGPASSWORD", "etlpass")
    user = os.getenv("PGUSER", "etl")
    url = (f"jdbc:postgresql://{os.getenv('PGHOST', 'postgres')}:"
           f"{os.getenv('PGPORT', '5432')}/{os.getenv('PGDATABASE', 'etldemo')}")

    for svc in services.get("controllerServices", []):
        if "DBCPConnectionPool" not in svc["component"]["type"]:
            continue
        nifi.update_controller_service(svc["id"], {
            "Database Connection URL": url,
            "Database User": user,
            "Password": password,
        })
        print(f"[provision] re-wired credentials on {svc['component']['name']}",
              flush=True)


def run() -> int:
    nifi = NiFi()
    nifi.wait_until_ready()
    root = nifi.root_id()

    group = nifi.find_child_group(root, GROUP_NAME)
    if group:
        print(f"[provision] '{GROUP_NAME}' already on the canvas -- reusing it")
        gid = group["id"]
    else:
        flow_file = os.getenv("FLOW_FILE", "/flow/ecommerce_etl.flow.json")
        if not os.path.exists(flow_file):
            print(f"[provision] no flow definition at {flow_file}.\n"
                  f"            Build one first:  make build-flow")
            return 1
        print(f"[provision] importing {flow_file}")
        imported = nifi.upload_flow_definition(root, flow_file, GROUP_NAME, (300, 120))
        gid = imported["id"]

    try:
        # Quiesce before reconfiguring: stop processors, then peel the services
        # apart in dependency order. Doing this on the re-run path too keeps
        # `docker compose up` idempotent instead of crash-looping.
        nifi.set_group_state(gid, "STOPPED")
        nifi.wait_for_processors_stopped(gid)
        nifi.disable_all_services(gid)

        wire_sensitive_properties(nifi, gid)

        nifi.enable_all_services(gid)
        if not nifi.wait_for_services_enabled(gid):
            print("[provision] controller services did not all enable -- check the "
                  "DB password and that nifi/drivers/postgresql.jar is mounted")
            return 1

        nifi.set_group_state(gid, "RUNNING")
        print(f"[provision] flow '{GROUP_NAME}' is RUNNING ({gid})")
    except NiFiError as exc:
        print(f"[provision] failed: {exc}")
        return 1

    return 0
