"""'Pipeline states' -- the literal thing the report is asked to show: what
NiFi's canvas looks like right now, and what the source-side job-run ledger
says about the same batches. Both readers are generic: the NiFi half reuses
migrator/ssis2nifi/deploy/nifi_api.py's NiFi client exactly as the vendored
converter's own deploy step does (no report-specific NiFi code), and the
ledger half only ever reads the table job.yml's `ledger:` block names.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "migrator"))

from ssis2nifi.deploy.nifi_api import NiFi  # noqa: E402
from ssis2nifi.deploy.provision import api_url  # noqa: E402


def nifi_group_state(nifi_url: str, group_name: str) -> dict:
    """RUNNING/STOPPED/MIXED for `group_name`, plus per-processor detail and
    any bulletins -- the same computation api/app.py's /flow-status endpoint
    already does in the reference repo, reused here read-only."""
    client = NiFi(api_url(nifi_url))
    root = client.root_id()
    group = client.find_child_group(root, group_name)
    if group is None:
        return {"found": False, "group_name": group_name}

    running, stopped, invalid = (group.get("runningCount", 0), group.get("stoppedCount", 0),
                                  group.get("invalidCount", 0))
    state = "RUNNING" if stopped == 0 and running else ("STOPPED" if running == 0 else "MIXED")

    # "Process Groups" resource, not "Flow" -- /flow/process-groups/{id} only
    # exposes the group's own overview/status/controller-services/bulletin
    # sub-paths; listing the processors INSIDE a group is
    # /process-groups/{id}/processors (no /flow prefix).
    processors = client.get(f"/process-groups/{group['id']}/processors").get("processors", [])
    proc_detail = [
        {
            "name": p["component"]["name"],
            "type": p["component"]["type"].rsplit(".", 1)[-1],
            "state": p["component"]["state"],
            "run_status": p.get("status", {}).get("aggregateSnapshot", {}).get("runStatus"),
        }
        for p in processors
    ]

    # Queue depth is a per-CONNECTION concept in NiFi, not per-processor;
    # the group's own status rollup already aggregates it across every
    # connection inside, which is what "how much is still in flight" means
    # at the pipeline-state level.
    status = client.get(f"/flow/process-groups/{group['id']}/status").get(
        "processGroupStatus", {}).get("aggregateSnapshot", {})
    queued_total = status.get("flowFilesQueued", 0)

    bulletins = client.get("/flow/bulletin-board").get("bulletinBoard", {}).get("bulletins", [])
    group_bulletins = [
        b["bulletin"]["message"] for b in bulletins
        if b.get("bulletin", {}).get("groupId") == group["id"]
    ]

    return {
        "found": True, "group_name": group_name, "group_id": group["id"], "state": state,
        "running": running, "stopped": stopped, "invalid": invalid, "queued_total": queued_total,
        "processors": proc_detail, "bulletins": group_bulletins,
    }


def source_job_runs(conn, ledger_table: str, limit: int = 20) -> list[dict]:
    """The most recent rows from the source-side ledger -- the "what did the
    SSIS-equivalent run report about itself" half of pipeline state."""
    with conn.cursor() as cur:
        cur.execute(  # noqa: S608 -- table name from job.yml's ledger.table
            f"SELECT batch_id, source_file, started_at, finished_at, "
            f"records_loaded, records_rejected, duration_ms, status "
            f"FROM {ledger_table} ORDER BY started_at DESC LIMIT %s",
            (limit,),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
