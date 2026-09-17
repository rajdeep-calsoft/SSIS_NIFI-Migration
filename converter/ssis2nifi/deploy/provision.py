"""Import a generated flow, put the secrets back, and start it.

WHY THIS STEP HAS TO EXIST
--------------------------
The generated flow.json deliberately contains no credentials -- sensitive
properties are null and a sidecar names the environment variable that supplies
each one. That is what makes the artifact safe to commit, and it is the same
thing NiFi's own export does, so an import sees nothing unusual.

The cost is that a freshly imported flow cannot connect to anything until the
values are put back. This does that, in the order NiFi actually requires:

    import -> stop -> set secrets -> enable services -> start

Services must be enabled AFTER their properties are set, because a running
service will not accept a property change; and a service cannot be enabled
before the services it references exist, which is why the generated flow
imports everything DISABLED and this turns them on in one pass afterwards.

Generalised from ~/Desktop/NIFI-FLOW/generator/gen/provision.py, which does the
same dance but with the three Postgres property names hardcoded. Here the
sidecar decides, so a package with four databases works without a code change.
"""

from __future__ import annotations

import json
import os
import pathlib
import tempfile
import time

from .nifi_api import NiFi, NiFiError

API_SUFFIX = "/nifi-api"


def api_url(base: str) -> str:
    base = base.rstrip("/")
    return base if base.endswith(API_SUFFIX) else base + API_SUFFIX


class MissingSecret(Exception):
    """A sensitive property has no value in the environment. Refuse to deploy."""


def _resolve(secrets: list[dict]) -> dict[str, dict[str, str]]:
    """service name -> {property: value}, read from the environment.

    Fails loudly and names every missing variable at once, rather than
    deploying a flow that will fail to connect for a reason nobody can see.
    """
    missing = [s["env_var"] for s in secrets if not os.getenv(s["env_var"])]
    if missing:
        raise MissingSecret(
            "no value in the environment for: " + ", ".join(sorted(set(missing)))
            + "\nSet them and re-run; they are never stored in the flow or the bindings."
        )
    out: dict[str, dict[str, str]] = {}
    for s in secrets:
        out.setdefault(s["service_name"], {})[s["property"]] = os.environ[s["env_var"]]
    return out


def deploy(flow_path: str, base_url: str, group_name: str | None = None,
           start: bool = True, replace: bool = True) -> dict:
    """Import, inject, enable and (optionally) start. Returns a small summary."""
    flow_file = pathlib.Path(flow_path)
    flow = json.loads(flow_file.read_text())
    name = group_name or flow["flowContents"]["name"]
    flow["flowContents"]["name"] = name

    side = flow_file.with_suffix("").with_suffix(".secrets.json")
    secrets = json.loads(side.read_text()) if side.exists() else []
    values = _resolve(secrets)

    nifi = NiFi(api_url(base_url))
    nifi.wait_until_ready()
    root = nifi.root_id()

    # Re-deploying must replace, not accumulate. Two copies of the same flow
    # both reading the same directory is a data-corrupting surprise.
    if replace:
        existing = nifi.find_child_group(root, name)
        if existing:
            nifi.delete_group(existing["id"])

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(flow, fh)
        staged = fh.name
    try:
        entity = nifi.upload_flow_definition(root, staged, name, (0, 1400))
        group_id = entity.get("id") or entity["component"]["id"]
    finally:
        pathlib.Path(staged).unlink(missing_ok=True)

    # Secrets, before anything is enabled.
    injected = 0
    for svc in nifi.get(f"/flow/process-groups/{group_id}/controller-services").get(
            "controllerServices", []):
        svc_name = svc["component"]["name"]
        if svc_name in values:
            nifi.update_controller_service(svc["id"], values[svc_name])
            injected += len(values[svc_name])

    nifi.enable_all_services(group_id)
    try:
        nifi.wait_for_services_enabled(group_id)
    except NiFiError:
        pass          # reported in the summary below rather than raised

    states = {
        s["component"]["name"]: s["component"]["state"]
        for s in nifi.get(f"/flow/process-groups/{group_id}/controller-services").get(
            "controllerServices", [])
    }

    if start:
        nifi.set_group_state(group_id, "RUNNING")
        time.sleep(2)

    return {
        "group_id": group_id,
        "group_name": name,
        "secrets_injected": injected,
        "services": states,
        "started": start,
    }
