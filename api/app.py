"""The curl-triggered middle layer.

    SSIS container (source)  --curl-->  THIS  -->  NiFi container (destination)

Section 6 of the manager's brief, in order:

    1. trigger via curl
    2. fetch the specific .dtsx from the source side
    3. convert it to a project-specific intermediate format (the IR)
    4. build the equivalent pipeline on the destination side

Steps 3-4 are not reimplemented here -- they ARE `converter/` (SSIS2NIFI),
already built, already proven (M1-M5). This file is a front door only: it
imports and calls converter/ssis2nifi's existing analyze/convert/deploy
functions directly (no subprocess, no shell-out), and adds exactly what did
not exist before -- something to `curl`.

Deliberately synchronous and stateless. One .dtsx package, one request,
done in a few seconds for /convert and under NiFi's own settle time for
/deploy (see converter's own `make deploy`, which does the same wait). No
job queue: a demo triggered by curl does not need one, and it is one more
thing that can drift from the converter it wraps.
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# converter/ is bind-mounted at /converter (see docker-compose.yml) so this
# always runs the SAME code `make convert` does on the host -- never a copy.
CONVERTER_ROOT = Path(os.getenv("CONVERTER_ROOT", "/converter"))
sys.path.insert(0, str(CONVERTER_ROOT))

from ssis2nifi.catalog.derive import derive          # noqa: E402
from ssis2nifi.catalog.support import annotate       # noqa: E402
from ssis2nifi.dtsx.parse import NotADtsxPackage, parse_file  # noqa: E402
from ssis2nifi.emit import flowdef                    # noqa: E402
from ssis2nifi.ir import schema                        # noqa: E402

SOURCE_PACKAGES = Path(os.getenv("SOURCE_PACKAGES", "/source-packages"))
BINDINGS_DIR = CONVERTER_ROOT / "bindings"
OUT_DIR = Path(os.getenv("OUT_DIR", "/out"))
DEFAULT_NIFI = os.getenv("NIFI_API", "http://destination-nifi:8080")
HANDBUILT_GROUP = os.getenv("HANDBUILT_GROUP", "ecommerce_etl")
# Every package this API deploys is a converter-GENERATED flow (never the
# hand-built ecommerce_etl), and plain tier3-bulk's full fault mix includes
# missing_field/wrong_type, which crashes a generated flow's schema-inferred
# record processors (IllegalTypeConversionException: Cannot convert CHOICE) --
# found running pkg_line_checks through it. tier3-bulk-generated is the one
# built to be safe for any generated package.
NEXT_STEP = "make bulk-run TIER=tier3-bulk-generated   # or: curl the demo runbook in docs/1.manager-demo.md"
STATIC_DIR = Path(__file__).parent / "static"

# The ONE real deployment target every package through this API lands on
# (DEFAULT_NIFI above is that same target's NiFi). These are not guesses --
# every hand-written bindings/*.bindings.yml file that targets destination/
# already hardcodes these identical values (see converter/bindings/
# pkg_orders_etl.bindings.yml). Auto-generation just stops making a human
# retype them for every new package.
GEN_DB_TYPE = os.getenv("GEN_DB_TYPE", "PostgreSQL")
GEN_DB_URL = os.getenv("GEN_DB_URL", "jdbc:postgresql://postgres:5432/etldemo")
GEN_DB_DRIVER_CLASS = os.getenv("GEN_DB_DRIVER_CLASS", "org.postgresql.Driver")
GEN_DB_DRIVER_PATH = os.getenv("GEN_DB_DRIVER_PATH", "/opt/nifi/drivers/postgresql.jar")
GEN_DB_USER = os.getenv("GEN_DB_USER", "etl")
GEN_DB_PASSWORD_REF = os.getenv("GEN_DB_PASSWORD_REF", "ORDERS_ETL_DB_MAIN_PASSWORD")
GEN_DB_IDENTIFIER_CASE = os.getenv("GEN_DB_IDENTIFIER_CASE", "lower")
GEN_LANDING_DIR = os.getenv("GEN_LANDING_DIR", "/opt/nifi/data/landing")

# For GET /export/rejects.csv -- a read-only connection to that same
# warehouse, reached the same way destination-nifi is (host-gateway,
# published port, never joining destination's own docker network).
DEST_PG_HOST = os.getenv("DEST_PG_HOST", "destination-postgres")
DEST_PG_PORT = os.getenv("DEST_PG_PORT", "5433")
DEST_PG_DB = os.getenv("DEST_PG_DB", "etldemo")
DEST_PG_USER = os.getenv("DEST_PG_USER", "etl")

app = FastAPI(
    title="ssis2nifi middle layer",
    description="curl-triggered: fetch a .dtsx from the source side, convert it, "
                 "deploy it onto the destination NiFi.",
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def control_panel():
    """The whole point of this route: click buttons instead of curling.
    Same-origin as every API call it makes, so no CORS setup is needed."""
    return FileResponse(STATIC_DIR / "index.html")


class ConvertRequest(BaseModel):
    package: str  # a filename under source-packages/, or an http(s) URL
    bindings: Optional[str] = None  # bindings filename under converter/bindings/; default <stem>.bindings.yml


class DeployRequest(ConvertRequest):
    nifi: Optional[str] = None
    group_name: Optional[str] = None
    start: bool = True


def _fetch_package(package: str) -> Path:
    """Resolve `package` to a local .dtsx path -- a name on the mounted
    source directory, or an http(s) URL fetched into a scratch file.
    Either way, the caller never needs to know which."""
    if package.startswith("http://") or package.startswith("https://"):
        dest = OUT_DIR / "_fetched" / Path(package).name
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            urllib.request.urlretrieve(package, dest)  # noqa: S310 -- explicit user-supplied URL
        except OSError as exc:
            raise HTTPException(502, f"could not fetch {package}: {exc}") from exc
        return dest

    path = (SOURCE_PACKAGES / package).resolve()
    if SOURCE_PACKAGES.resolve() not in path.parents and path != SOURCE_PACKAGES.resolve():
        raise HTTPException(400, "package must be a plain filename, not a path")
    if not path.is_file():
        raise HTTPException(404, f"{package!r} not found under source packages "
                                  f"({SOURCE_PACKAGES}); see GET /packages")
    return path


@dataclass
class BindingsResolution:
    path: Path
    generated: bool
    warnings: list[str] = field(default_factory=list)


def _resolve_bindings(pkg_path: Path, bindings: Optional[str]) -> BindingsResolution:
    """Find (or, for the default filename only, auto-generate) this
    package's bindings file. An explicitly-named bindings file that doesn't
    exist still refuses loudly -- auto-generation only fires when nobody
    named a specific file, so it can never silently substitute for one a
    caller deliberately asked for."""
    explicit = bindings is not None
    name = bindings or f"{pkg_path.stem}.bindings.yml"
    path = (BINDINGS_DIR / name).resolve()
    if BINDINGS_DIR.resolve() not in path.parents:
        raise HTTPException(400, "bindings must be a plain filename")

    if path.is_file():
        return BindingsResolution(path=path, generated=False)

    if explicit:
        raise HTTPException(404, f"no bindings file {name!r} in {BINDINGS_DIR} -- "
                                  f"pass 'bindings' explicitly, or add one")

    try:
        pkg = parse_file(str(pkg_path))
    except NotADtsxPackage as exc:
        raise HTTPException(422, f"refused: {exc}") from exc

    warnings = _write_generated_bindings(path, pkg_path, pkg)
    print(f"[bindings] auto-generated {path} for {pkg_path.name}"
          + (f" ({len(warnings)} warning(s))" if warnings else ""), file=sys.stderr)
    return BindingsResolution(path=path, generated=True, warnings=warnings)


def _slug(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return s or "conn"


def _unique_key(ref_id: str, used: set[str]) -> str:
    m = re.search(r"\[([^\]]+)\]\s*$", ref_id)
    base = _slug(m.group(1) if m else ref_id)
    key, i = base, 2
    while key in used:
        key, i = f"{base}_{i}", i + 1
    used.add(key)
    return key


def _oledb_binding_block(conn) -> dict:
    return {
        "for": conn.ref_id,
        "db_type": GEN_DB_TYPE,
        "identifier_case": GEN_DB_IDENTIFIER_CASE,
        "url": GEN_DB_URL,
        "driver_class": GEN_DB_DRIVER_CLASS,
        "driver_path": GEN_DB_DRIVER_PATH,
        "user": GEN_DB_USER,
        "password_ref": GEN_DB_PASSWORD_REF,
    }


def _flatfile_binding_block(conn) -> tuple[dict, Optional[str]]:
    """directory is this API's one fixed landing dir. file_filter/reader_kind
    come from the extension already present in the connection manager's own
    ConnectionString -- not invented."""
    raw = conn.properties.get("ConnectionString", "")
    ext = Path(raw.replace("\\", "/")).suffix.lstrip(".").lower()
    block = {"for": conn.ref_id, "directory": GEN_LANDING_DIR}
    if not ext:
        block["file_filter"] = ".*"
        return block, (f"connection {conn.ref_id!r}: no file extension found in "
                        f"ConnectionString {raw!r}; file_filter defaulted to '.*' "
                        "-- please review by hand")
    if ext == "ndjson":
        # Real landing files here are never actually named *.ndjson: this
        # project's own generator/fixture tooling (destination/generator/gen/
        # emit.py, spec/fixtures/inject_fixture.py) always writes *.json even
        # though the content is newline-delimited JSON. Match both, or a
        # package deployed through here would never see files the shared
        # generator/`make bulk-run` actually produces -- discovered the hard
        # way running pkg_full_coverage_synthetic through `make bulk-run`.
        block["file_filter"] = ".*\\.(json|ndjson)"
        block["reader_kind"] = "ndjson"
    else:
        block["file_filter"] = f".*\\.{ext}"
    # any other extension: reader_kind stays unset -> flowdef's existing
    # `when: binding.reader_kind != 'ndjson'` picks the CSVReader/delimited
    # path, which self-configures from the package's own Format/
    # FlatFileColumns metadata via derive.py. No guess needed here either.
    return block, None


def _generate_bindings_yaml(pkg_path: Path, pkg) -> tuple[str, list[str]]:
    used: set[str] = set()
    entries: list[tuple[str, dict]] = []
    warnings: list[str] = []

    for conn in pkg.connections:
        if conn.kind == "OLEDB":
            entries.append((_unique_key(conn.ref_id, used), _oledb_binding_block(conn)))
        elif conn.kind == "FLATFILE":
            block, warn = _flatfile_binding_block(conn)
            entries.append((_unique_key(conn.ref_id, used), block))
            if warn:
                warnings.append(warn)
        else:
            # Anything else (ADONET, EXCEL, ODBC, UNKNOWN, ...): deliberately
            # left out. flowdef.build()'s existing EmitError will name this
            # connection explicitly and refuse, rather than this generator
            # inventing a binding for a kind it has no fixed-target facts for.
            warnings.append(
                f"connection {conn.ref_id!r} has kind {conn.kind!r} ({conn.creation_name!r}), "
                "which this generator does not know how to bind and has left out; "
                "any component needing it will fail conversion with a clear error "
                "naming it, not a guessed binding"
            )

    header = (
        f"# AUTO-GENERATED by api/app.py._resolve_bindings on "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        f"# Source package: {pkg_path.name}\n"
        "#\n"
        "# Generated from (a) this API's one fixed deployment target (destination\n"
        "# Postgres + landing dir -- see api/app.py's GEN_* constants) and (b) facts\n"
        "# already present in the package's own connection managers. Nothing here\n"
        "# was guessed.\n"
        "#\n"
        "# Freely hand-edit or delete this file -- deleting it regenerates it the\n"
        "# same way on the next /convert or /deploy for this package.\n"
        "#\n"
        "# NO SECRETS HERE. password_ref names an environment variable; the value\n"
        "# is never written into this file or into the generated flow.json.\n"
    )
    if warnings:
        header += "#\n# Generator warnings (review before trusting this file blindly):\n"
        header += "".join(f"#  - {w}\n" for w in warnings)

    body = yaml.safe_dump({"bindings": dict(entries)}, sort_keys=False, default_flow_style=False)
    return header + "\n" + body, warnings


def _write_generated_bindings(path: Path, pkg_path: Path, pkg) -> list[str]:
    yaml_text, warnings = _generate_bindings_yaml(pkg_path, pkg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml_text)
    return warnings


def _convert(pkg_path: Path, bindings_path: Path, group_name: Optional[str]) -> dict:
    """Stages A+B: .dtsx -> IR -> flow.json. Exactly what `make convert` does."""
    try:
        pkg = derive(annotate(parse_file(str(pkg_path))))
    except NotADtsxPackage as exc:
        raise HTTPException(422, f"refused: {exc}") from exc

    result = {
        "package": pkg_path.name,
        "content_digest": schema.content_digest(pkg)[:16],
        "topology_digest": schema.topology_digest(pkg)[:16],
        "coverage": {
            "all_recognised": pkg.coverage.all_recognised,
            "recognised": pkg.coverage.recognised,
            "total": pkg.coverage.total_components,
        },
    }

    if not pkg.coverage.all_recognised:
        result["exit_code"] = 3
        result["exit_meaning"] = "parsed, but something in this package needs a human review"
        result["flow_path"] = None
        return result

    bindings = flowdef.load_bindings(str(bindings_path))
    try:
        flow, secrets, notes = flowdef.build(pkg, bindings, group_name)
    except flowdef.EmitError as exc:
        raise HTTPException(422, f"cannot generate a flow: {exc}") from exc

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    flow_path = OUT_DIR / f"{pkg_path.stem}.flow.json"
    flow_path.write_text(json.dumps(flow, indent=2))

    fc = flow["flowContents"]
    result.update({
        "exit_code": 0,
        "exit_meaning": "every component in the package converted cleanly",
        "flow_path": str(flow_path),
        "processors": len(fc["processors"]),
        "connections": len(fc["connections"]),
        "controller_services": len(fc["controllerServices"]),
        "notes": notes,
    })

    if secrets:
        secrets_path = flow_path.with_suffix("").with_suffix(".secrets.json")
        secrets_path.write_text(json.dumps(secrets, indent=2))
        result["secrets_needed"] = sorted({s["env_var"] for s in secrets})
    else:
        result["secrets_needed"] = []

    return result


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/packages")
def list_packages():
    """What's fetchable from the source side right now."""
    if not SOURCE_PACKAGES.is_dir():
        raise HTTPException(500, f"source packages directory not mounted at {SOURCE_PACKAGES}")
    return {"packages": sorted(p.name for p in SOURCE_PACKAGES.glob("*.dtsx"))}


@app.get("/export/rejects.csv")
def export_rejects_csv():
    """One click, no Grafana Inspect drawer: every rejected record, both
    engines, full untruncated reason -- the same query as the dashboard's
    own 'Export: every rejected record, full detail (CSV)' panel
    (destination/grafana/dashboards/engine-comparison.json), served
    directly as a file download."""
    try:
        conn = psycopg2.connect(
            host=DEST_PG_HOST, port=DEST_PG_PORT, dbname=DEST_PG_DB,
            user=DEST_PG_USER, password=os.environ["ORDERS_ETL_DB_MAIN_PASSWORD"],
        )
    except psycopg2.OperationalError as exc:
        raise HTTPException(502, f"could not reach the destination warehouse: {exc}") from exc

    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                SELECT date_trunc('second', at) AS at, source_file, order_id,
                       line_no, sku, qty, unit_price, currency,
                       nifi_says AS "NiFi says", ssis_says AS "SSIS says",
                       agree, why AS reason
                  FROM v_reject_side_by_side
                 ORDER BY (agree = 'same'), at DESC, order_id
            """)
            rows = cur.fetchall()
            headers = [d.name for d in cur.description]
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    writer.writerows(rows)

    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="rejected_records.csv"'},
    )


@app.post("/convert")
def convert(req: ConvertRequest):
    """Steps 2-3 of the manager's brief: fetch the .dtsx, convert it.
    Does not touch NiFi -- pure, offline, safe to call repeatedly."""
    pkg_path = _fetch_package(req.package)
    resolution = _resolve_bindings(pkg_path, req.bindings)
    result = _convert(pkg_path, resolution.path, group_name=None)
    result["bindings"] = {"file": resolution.path.name, "generated": resolution.generated,
                           "warnings": resolution.warnings}
    return result


@app.post("/deploy")
def deploy(req: DeployRequest):
    """All four steps: fetch, convert, and build the equivalent pipeline on
    the destination NiFi. On success, tells the developer what to run next --
    exactly the manager's "this is ready, please run the test pipeline"."""
    from ssis2nifi.deploy.provision import MissingSecret, deploy as provision_deploy

    pkg_path = _fetch_package(req.package)
    resolution = _resolve_bindings(pkg_path, req.bindings)
    group_name = req.group_name or f"ssis2nifi-{pkg_path.stem}"

    convert_result = _convert(pkg_path, resolution.path, group_name)
    convert_result["bindings"] = {"file": resolution.path.name, "generated": resolution.generated,
                                   "warnings": resolution.warnings}
    if convert_result["exit_code"] != 0:
        convert_result["message"] = ("not deployed -- this package needs human review first "
                                      "(see 'exit_meaning')")
        return convert_result

    # Stop every OTHER running flow before this one starts, so a deploy can
    # never leave two (or three) groups RUNNING at once by accident.
    stopped_others = _stop_other_running_groups(group_name, req.nifi) if req.start else []

    try:
        deploy_result = provision_deploy(
            convert_result["flow_path"], req.nifi or DEFAULT_NIFI,
            group_name, start=req.start,
        )
    except MissingSecret as exc:
        raise HTTPException(424, f"refusing to deploy: {exc}") from exc

    unsettled = [s["group_name"] for s in stopped_others if not s["settled"]]
    message = f"ready — please run the test pipeline:  {NEXT_STEP}"
    if unsettled:
        message = (f"deployed, but still finishing a stop on {unsettled} -- it has a large "
                    "in-flight batch draining (normal, can take minutes on a big backlog); "
                    "check GET /flow-status before trusting result counts")

    return {
        **convert_result,
        "stopped_other_flows": stopped_others,
        "deployed": deploy_result,
        "message": message,
    }


# ---------------------------------------------------------------------------
# Flow toggle -- exactly one flow may run on destination NiFi at a time (the
# hand-built and generated flows write the same tables). Same rule
# scripts/*.sh enforce from the terminal; these endpoints are the same
# operations for the control panel, so a beginner never needs the terminal
# for the everyday "which flow is live, switch it" loop.
# ---------------------------------------------------------------------------

def _nifi_client(nifi_url: Optional[str] = None):
    from ssis2nifi.deploy.nifi_api import NiFi
    from ssis2nifi.deploy.provision import api_url
    return NiFi(api_url(nifi_url or DEFAULT_NIFI))


def _set_group_state(group_name: str, state: str, nifi_url: Optional[str]) -> dict:
    """Set one group's state. For STOPPED, waits and verifies it actually
    got there (same reasoning as _stop_other_running_groups below) instead
    of trusting NiFi's instant response to a stop command -- otherwise a
    manual Stop click can report success while a big in-flight batch is
    still draining, the same stale-MIXED trap this whole mechanism exists
    to avoid."""
    import time

    client = _nifi_client(nifi_url)
    root = client.root_id()
    grp = client.find_child_group(root, group_name)
    if grp is None:
        return {"group_name": group_name, "found": False,
                "message": "not on the canvas -- nothing to do"}
    client.set_group_state(grp["id"], state)

    if state != "STOPPED":
        return {"group_name": group_name, "found": True, "state": state}

    settled = False
    for _ in range(30):  # ~60s budget, same as _stop_other_running_groups
        data = client.get(f"/flow/process-groups/{root}")
        g = next((x for x in data["processGroupFlow"]["flow"]["processGroups"]
                  if x["id"] == grp["id"]), None)
        if g is None or g.get("runningCount", 0) == 0:
            settled = True
            break
        time.sleep(2)
    return {"group_name": group_name, "found": True, "state": state, "settled": settled}


def _stop_other_running_groups(keep_group_name: str, nifi_url: Optional[str]) -> list[dict]:
    """Enforce 'exactly one flow live on destination NiFi' automatically,
    every time something is about to start running -- not just when someone
    remembers to use the Advanced toggle first. Without this, deploying
    package after package silently piles up RUNNING groups that all write
    the same warehouse tables (found the hard way: three at once).

    WAITS for each stop to actually finish before returning, the same
    ask-then-verify-then-retry shape scripts/flow_state.sh uses. NiFi accepts
    a stop command instantly but a processor can keep running its last
    in-flight batch for a while afterward (see this repo's own
    wait_for_settle note on Check Replay) -- firing the stop and returning
    immediately is exactly how a caller ends up looking at a canvas that
    still reads MIXED a moment after being told it was handled.
    """
    import time

    client = _nifi_client(nifi_url)
    root = client.root_id()

    def snapshot() -> dict:
        data = client.get(f"/flow/process-groups/{root}")
        return {g["component"]["name"]: g for g in data["processGroupFlow"]["flow"]["processGroups"]}

    groups = snapshot()
    targets = {name: g["id"] for name, g in groups.items()
               if name != keep_group_name and g.get("runningCount", 0) > 0}
    for gid in targets.values():
        client.set_group_state(gid, "STOPPED")

    remaining = set(targets)
    for _ in range(30):  # ~60s budget -- covers a normal stop; a huge
        if not remaining:  # leftover in-flight batch can still take longer,
            break           # which is reported honestly below, not hidden.
        current = snapshot()
        for name in list(remaining):
            g = current.get(name)
            if g is None or g.get("runningCount", 0) == 0:
                remaining.discard(name)
        if remaining:
            time.sleep(2)

    return [{"group_name": name, "settled": name not in remaining} for name in targets]


class FlowStateRequest(BaseModel):
    group_name: str
    nifi: Optional[str] = None


class UseGeneratedRequest(BaseModel):
    package: str = "pkg_orders_etl.dtsx"
    bindings: Optional[str] = None
    nifi: Optional[str] = None


class UseHandbuiltRequest(BaseModel):
    generated_group: str = "ssis2nifi-pkg_orders_etl"
    handbuilt_group: str = HANDBUILT_GROUP
    nifi: Optional[str] = None


@app.get("/flow-status")
def flow_status(nifi: Optional[str] = None):
    """Every process group on destination NiFi's canvas and its state --
    what `scripts/which-flow.sh` prints, as JSON."""
    client = _nifi_client(nifi)
    root = client.root_id()
    data = client.get(f"/flow/process-groups/{root}")
    groups, live = [], []
    for g in data["processGroupFlow"]["flow"]["processGroups"]:
        running, stopped, invalid = (g.get("runningCount", 0), g.get("stoppedCount", 0),
                                      g.get("invalidCount", 0))
        state = "RUNNING" if stopped == 0 and running else ("STOPPED" if running == 0 else "MIXED")
        groups.append({"name": g["component"]["name"], "id": g["id"], "state": state,
                        "running": running, "stopped": stopped, "invalid": invalid})
        if state == "RUNNING":
            live.append(g["component"]["name"])
    return {"groups": groups, "live": live,
            "warning": (f"more than one group is RUNNING at once: {live} -- both write the "
                        "same warehouse tables" if len(live) > 1 else None)}


@app.post("/flow/stop")
def flow_stop(req: FlowStateRequest):
    return _set_group_state(req.group_name, "STOPPED", req.nifi)


class StopAllRequest(BaseModel):
    nifi: Optional[str] = None


@app.post("/flow/stop-all")
def flow_stop_all(req: StopAllRequest = StopAllRequest()):
    """Stop every group on the canvas, whatever it's named -- the general
    escape hatch for 'several flows have piled up and I just want a clean
    canvas', not only the fixed hand-built/generated pair."""
    stopped = _stop_other_running_groups(keep_group_name="", nifi_url=req.nifi)
    return {"stopped_flows": stopped}


@app.post("/flow/start")
def flow_start(req: FlowStateRequest):
    stopped_others = _stop_other_running_groups(req.group_name, req.nifi)
    result = _set_group_state(req.group_name, "RUNNING", req.nifi)
    return {**result, "stopped_other_flows": stopped_others}


@app.post("/flow/use-generated")
def use_generated_flow(req: UseGeneratedRequest = UseGeneratedRequest()):
    """Stop the hand-built flow, then convert+deploy the generated one --
    the control-panel equivalent of `make use-generated`."""
    stopped = _set_group_state(HANDBUILT_GROUP, "STOPPED", req.nifi)
    deployed = deploy(DeployRequest(package=req.package, bindings=req.bindings, nifi=req.nifi))
    return {"stopped_handbuilt": stopped, **deployed}


@app.post("/flow/use-handbuilt")
def use_handbuilt_flow(req: UseHandbuiltRequest = UseHandbuiltRequest()):
    """Stop the generated flow, restart the hand-built one -- the
    control-panel equivalent of `make use-handbuilt`."""
    stopped = _set_group_state(req.generated_group, "STOPPED", req.nifi)
    started = _set_group_state(req.handbuilt_group, "RUNNING", req.nifi)
    return {"stopped_generated": stopped, "started_handbuilt": started}
