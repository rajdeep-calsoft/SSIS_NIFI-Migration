# VENDORED -- do not edit here.
#
# Copied verbatim from ~/Desktop/NIFI-FLOW/generator/gen/nifi_api.py at commit
# 4f3e92c. It is a zero-dependency REST client living inside a Docker-only,
# non-installable package, so importing it across repos would need a sys.path
# hack and would make this tool unshippable to a customer.
#
# `make check-vendored` diffs this against the original and fails on drift.
# Fix drift by re-copying, never by editing this file.

"""Thin NiFi 1.x REST client.

NiFi's API is revision-based optimistic locking: every mutation must carry the
entity's current revision version, so most helpers here do a GET before a PUT.
Because the demo runs NiFi unsecured over HTTP there is no token handling --
that is the whole reason we chose the unsecured profile.
"""
from __future__ import annotations

import os
import time
from typing import Any

import requests


class NiFiError(RuntimeError):
    pass


class NiFi:
    def __init__(self, base: str | None = None, timeout: int = 30):
        self.base = (base or os.getenv("NIFI_API", "http://nifi:8080/nifi-api")).rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    # ---------------- low level ----------------

    def _request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{self.base}{path}"
        resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
        if resp.status_code >= 400:
            raise NiFiError(f"{method} {path} -> {resp.status_code}: {resp.text[:500]}")
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError:
            return resp.text

    def get(self, path: str, **kw) -> Any:
        return self._request("GET", path, **kw)

    def post(self, path: str, body: dict | None = None, **kw) -> Any:
        return self._request("POST", path, json=body, **kw)

    def put(self, path: str, body: dict, **kw) -> Any:
        return self._request("PUT", path, json=body, **kw)

    def delete(self, path: str, **kw) -> Any:
        return self._request("DELETE", path, **kw)

    # ---------------- readiness ----------------

    def wait_until_ready(self, retries: int = 60, delay: float = 5.0) -> None:
        """NiFi answers /system-diagnostics well before the flow controller has
        finished initialising, so we also require the root process group to be
        addressable before declaring it ready."""
        last = None
        for attempt in range(1, retries + 1):
            try:
                self.get("/flow/process-groups/root")
                print(f"[nifi] ready after {attempt} attempt(s)", flush=True)
                return
            except Exception as exc:  # noqa: BLE001
                last = exc
                print(f"[nifi] waiting ({attempt}/{retries})", flush=True)
                time.sleep(delay)
        raise NiFiError(f"NiFi never became ready: {last}")

    # ---------------- lookups ----------------

    def root_id(self) -> str:
        return self.get("/flow/process-groups/root")["processGroupFlow"]["id"]

    def find_child_group(self, parent_id: str, name: str) -> dict | None:
        flow = self.get(f"/flow/process-groups/{parent_id}")["processGroupFlow"]["flow"]
        for group in flow.get("processGroups", []):
            if group["component"]["name"] == name:
                return group
        return None

    def entity(self, kind: str, entity_id: str) -> dict:
        """kind: processors | connections | controller-services | process-groups | funnels"""
        return self.get(f"/{kind}/{entity_id}")

    def revision_of(self, kind: str, entity_id: str) -> dict:
        return self.entity(kind, entity_id)["revision"]

    # ---------------- creation ----------------

    def create_processor(self, pg_id: str, ptype: str, name: str,
                         position: tuple[int, int], config: dict | None = None) -> dict:
        body = {
            "revision": {"version": 0},
            "component": {
                "type": ptype,
                "name": name,
                "position": {"x": position[0], "y": position[1]},
                "config": config or {},
            },
        }
        return self.post(f"/process-groups/{pg_id}/processors", body)

    def create_controller_service(self, pg_id: str, stype: str, name: str,
                                  properties: dict | None = None) -> dict:
        body = {
            "revision": {"version": 0},
            "component": {"type": stype, "name": name, "properties": properties or {}},
        }
        return self.post(f"/process-groups/{pg_id}/controller-services", body)

    def create_funnel(self, pg_id: str, position: tuple[int, int]) -> dict:
        body = {"revision": {"version": 0},
                "component": {"position": {"x": position[0], "y": position[1]}}}
        return self.post(f"/process-groups/{pg_id}/funnels", body)

    def create_process_group(self, parent_id: str, name: str,
                             position: tuple[int, int] = (0, 0)) -> dict:
        body = {"revision": {"version": 0},
                "component": {"name": name,
                              "position": {"x": position[0], "y": position[1]}}}
        return self.post(f"/process-groups/{parent_id}/process-groups", body)

    def connect(self, pg_id: str, source: dict, dest: dict,
                relationships: list[str], name: str = "",
                backpressure_count: int | None = None) -> dict:
        component = {
            "source": source,
            "destination": dest,
            "selectedRelationships": relationships,
            "name": name,
            "flowFileExpiration": "0 sec",
        }
        if backpressure_count is not None:
            component["backPressureObjectThreshold"] = backpressure_count
        body = {"revision": {"version": 0}, "component": component}
        return self.post(f"/process-groups/{pg_id}/connections", body)

    # ---------------- mutation ----------------

    def update_processor(self, processor_id: str, component_patch: dict) -> dict:
        current = self.entity("processors", processor_id)
        body = {
            "revision": current["revision"],
            "component": {"id": processor_id, **component_patch},
        }
        return self.put(f"/processors/{processor_id}", body)

    def update_controller_service(self, service_id: str, properties: dict) -> dict:
        current = self.entity("controller-services", service_id)
        body = {
            "revision": current["revision"],
            "component": {"id": service_id, "properties": properties},
        }
        return self.put(f"/controller-services/{service_id}", body)

    def set_service_state(self, service_id: str, state: str) -> dict:
        current = self.entity("controller-services", service_id)
        body = {"revision": current["revision"], "state": state,
                "disconnectedNodeAcknowledged": False}
        return self.put(f"/controller-services/{service_id}/run-status", body)

    def enable_all_services(self, pg_id: str) -> None:
        services = self.get(f"/flow/process-groups/{pg_id}/controller-services")
        for svc in services.get("controllerServices", []):
            sid = svc["id"]
            state = svc["component"]["state"]
            if state == "ENABLED":
                continue
            try:
                self.set_service_state(sid, "ENABLED")
                print(f"[nifi] enabled service {svc['component']['name']}", flush=True)
            except NiFiError as exc:
                print(f"[nifi] FAILED to enable {svc['component']['name']}: {exc}",
                      flush=True)

    def wait_for_services_enabled(self, pg_id: str, retries: int = 30,
                                  delay: float = 2.0) -> bool:
        for _ in range(retries):
            services = self.get(f"/flow/process-groups/{pg_id}/controller-services")
            states = {s["component"]["name"]: s["component"]["state"]
                      for s in services.get("controllerServices", [])}
            if states and all(v == "ENABLED" for v in states.values()):
                return True
            time.sleep(delay)
        print(f"[nifi] services not all enabled: {states}", flush=True)
        return False

    def wait_for_services_disabled(self, pg_id: str, retries: int = 30,
                                   delay: float = 1.0) -> bool:
        states: dict[str, str] = {}
        for _ in range(retries):
            services = self.get(f"/flow/process-groups/{pg_id}/controller-services")
            states = {s["component"]["name"]: s["component"]["state"]
                      for s in services.get("controllerServices", [])}
            if not states or all(v == "DISABLED" for v in states.values()):
                return True
            time.sleep(delay)
        print(f"[nifi] services still not disabled: {states}", flush=True)
        return False

    def wait_for_processors_stopped(self, pg_id: str, retries: int = 30,
                                    delay: float = 1.0) -> bool:
        for _ in range(retries):
            flow = self.get(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
            running = [p["component"]["name"] for p in flow.get("processors", [])
                       if p["component"]["state"] == "RUNNING"]
            if not running:
                return True
            time.sleep(delay)
        print(f"[nifi] processors still running: {running}", flush=True)
        return False

    def empty_queues(self, pg_id: str) -> int:
        """Drop every FlowFile queued in the group. NiFi will not delete a
        group that still has data sitting in a connection."""
        flow = self.get(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
        dropped = 0
        for conn in flow.get("connections", []):
            cid = conn["id"]
            try:
                req = self.post(f"/flowfile-queues/{cid}/drop-requests")
            except NiFiError:
                continue
            request_id = req["dropRequest"]["id"]
            for _ in range(20):
                status = self.get(f"/flowfile-queues/{cid}/drop-requests/{request_id}")
                if status["dropRequest"].get("finished"):
                    break
                time.sleep(0.5)
            try:
                self.delete(f"/flowfile-queues/{cid}/drop-requests/{request_id}")
            except NiFiError:
                pass
            dropped += 1
        return dropped

    def disable_all_services(self, pg_id: str, passes: int = 8) -> bool:
        """Disable every controller service in the group.

        Order matters and is not knowable up front: the lookup services
        reference the connection pool, and NiFi refuses to disable a service
        while an enabled service still references it. Rather than model the
        dependency graph, retry in passes until nothing is left enabled --
        each pass peels off one layer.
        """
        states: dict[str, str] = {}
        for _ in range(passes):
            services = self.get(f"/flow/process-groups/{pg_id}/controller-services")
            states = {}
            pending = []
            for svc in services.get("controllerServices", []):
                state = svc["component"]["state"]
                states[svc["component"]["name"]] = state
                if state != "DISABLED":
                    pending.append(svc)
            if not pending:
                return True
            for svc in pending:
                try:
                    self.set_service_state(svc["id"], "DISABLED")
                except NiFiError:
                    pass  # still referenced; a later pass will get it
            time.sleep(1.5)
        print(f"[nifi] services still enabled after {passes} passes: {states}",
              flush=True)
        return False

    def delete_group(self, pg_id: str) -> None:
        """Stop everything, disable every service, drain the queues, then
        remove the group. Each step is asynchronous and NiFi refuses the next
        one until the previous has actually settled, so each is waited out."""
        self.set_group_state(pg_id, "STOPPED")
        self.wait_for_processors_stopped(pg_id)
        self.disable_all_services(pg_id)
        self.empty_queues(pg_id)

        rev = self.entity("process-groups", pg_id)["revision"]["version"]
        self.delete(f"/process-groups/{pg_id}?version={rev}&clientId=build")

    def set_group_state(self, pg_id: str, state: str) -> dict:
        """state: RUNNING | STOPPED -- applies to every processor in the group."""
        return self.put(f"/flow/process-groups/{pg_id}",
                        {"id": pg_id, "state": state,
                         "disconnectedNodeAcknowledged": False})

    # ---------------- flow definition import / export ----------------

    def download_flow_definition(self, pg_id: str) -> Any:
        return self.get(f"/process-groups/{pg_id}/download",
                        params={"includeReferencedServices": "true"})

    def upload_flow_definition(self, parent_id: str, path: str, group_name: str,
                               position: tuple[int, int] = (200, 100)) -> dict:
        with open(path, "rb") as fh:
            files = {"file": (os.path.basename(path), fh, "application/json")}
            data = {
                "groupName": group_name,
                "positionX": str(position[0]),
                "positionY": str(position[1]),
                "clientId": "bootstrap",
                "disconnectedNodeAcknowledged": "false",
            }
            return self._request(
                "POST", f"/process-groups/{parent_id}/process-groups/upload",
                files=files, data=data,
            )
