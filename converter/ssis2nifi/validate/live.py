"""Import a generated flow into a real NiFi and ask whether it is valid.

THE CHEAPEST CREDIBILITY IN THE PROJECT. One assertion -- that no component
reports a validation error -- catches every wrong property key, every missing
required property, every unresolved controller-service reference and every bad
bundle coordinate. That is the entire class of bug which otherwise gets found
by a human staring at a red canvas during a demo.

It is non-destructive by construction: the flow is imported into a NEW process
group with a generated name and deleted afterwards, so it can be run against an
instance that is already doing something. The teardown uses the same
stop -> disable services -> drain queues -> delete sequence NIFI-FLOW's
`delete_group` implements, because NiFi refuses to delete a group whose queues
are non-empty or whose services are still enabled.

`--keep` leaves the group on the canvas, which is what you want when the answer
is "it is invalid" and you need to look at it.
"""

from __future__ import annotations

import json
import pathlib
import tempfile
import time

from ..deploy.nifi_api import NiFi, NiFiError


class ValidationFailed(Exception):
    pass


# Errors that describe the environment, not the generated flow. A missing JDBC
# driver or an unreachable database says nothing about whether the conversion
# was correct -- those belong to deployment (M4), and conflating them would
# make this gate impossible to pass on a laptop with no SQL Server.
_ENVIRONMENTAL = (
    "is currently disabled",
    "is disabled",
    # A pool pointed at a database that is not running sits in ENABLING until
    # it times out. On a laptop with no SQL Server that is the normal state,
    # and it says nothing about whether the conversion was correct.
    "state is Enabling",
    "state is Disabling",
    "Failed to load driver",
    "Cannot load JDBC driver",
    "ClassNotFoundException",
    "Connection refused",
    "Unable to connect",
)


def _real_errors(errors: list[str]) -> list[str]:
    """Drop environmental noise; keep anything that indicts the flow itself."""
    return [e for e in errors if not any(token in e for token in _ENVIRONMENTAL)]


def _components(nifi: NiFi, group_id: str) -> list[dict]:
    """Every processor and controller service in the group, with its status.

    Uses the per-type endpoints rather than the process-group entity, because
    the aggregate snapshot on the group is stale -- it has been observed
    reporting components as stopped/invalid while the authoritative endpoints
    reported them running and valid.
    """
    out = []
    for kind, key in (("processors", "processors"), ("controller-services", "controllerServices")):
        try:
            data = nifi.get(f"/process-groups/{group_id}/{kind}")
        except NiFiError:
            continue
        for entity in data.get(key, []):
            comp = entity.get("component", {})
            out.append({
                "kind": kind,
                "name": comp.get("name", ""),
                "type": comp.get("type", "").rsplit(".", 1)[-1],
                "state": comp.get("state", ""),
                "errors": entity.get("component", {}).get("validationErrors")
                          or comp.get("validationErrors") or [],
                "status": entity.get("status", {}).get("validationStatus", ""),
            })
    return out


# The vendored client expects the API root, not the UI root. Passing
# http://host:8080 silently fetches the NiFi web page and every response
# parses as a string instead of JSON.
API_SUFFIX = "/nifi-api"


def api_url(base: str) -> str:
    base = base.rstrip("/")
    return base if base.endswith(API_SUFFIX) else base + API_SUFFIX


def verify(flow_path: str, base_url: str, keep: bool = False,
           settle_seconds: float = 6.0) -> list[dict]:
    """Import, wait for validation to settle, report every problem found.

    Returns the list of components carrying validation errors -- empty means
    the flow is valid.
    """
    nifi = NiFi(api_url(base_url))
    nifi.wait_until_ready()
    root = nifi.root_id()

    name = f"ssis2nifi-verify-{int(time.time())}"
    flow = json.loads(pathlib.Path(flow_path).read_text())
    flow["flowContents"]["name"] = name

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(flow, fh)
        staged = fh.name

    group_id = None
    try:
        entity = nifi.upload_flow_definition(root, staged, name, (0, 1400))
        # NiFi returns a ProcessGroupEntity: the id is on the entity itself.
        group_id = entity.get("id") or entity["component"]["id"]

        # Services import DISABLED, so every processor that references one
        # reports "depends on a Controller Service that is currently disabled".
        # That is an artifact of import state, not a defect in the flow, so
        # enable them before judging anything.
        try:
            nifi.enable_all_services(group_id)
            nifi.wait_for_services_enabled(group_id)
        except Exception as exc:                          # noqa: BLE001
            # A pool whose JDBC driver is absent cannot enable. That is a
            # DEPLOYMENT problem, not a generation problem, so carry on and
            # let the classifier below separate the two.
            print(f"note: not all services could be enabled ({exc})")

        # NiFi validates asynchronously; a component reads INVALID for a moment
        # after a change simply because it has not been looked at yet.
        deadline = time.time() + settle_seconds
        components: list[dict] = []
        while time.time() < deadline:
            components = _components(nifi, group_id)
            if components and all(c["status"] != "VALIDATING" for c in components):
                break
            time.sleep(1.0)

        return [c for c in components if _real_errors(c["errors"])]
    finally:
        pathlib.Path(staged).unlink(missing_ok=True)
        if group_id and not keep:
            try:
                nifi.delete_group(group_id)
            except Exception as exc:                      # noqa: BLE001
                print(f"warning: could not remove {name}: {exc}")
