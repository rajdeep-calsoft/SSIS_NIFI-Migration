#!/usr/bin/env python3
"""
Extract an engine-neutral pipeline spec, plus the NiFi binding for it, out of
the exported NiFi flow.

    python3 spec/extract_nifi.py            # write spec/pipeline.yml + spec/engines/nifi.yml
    python3 spec/extract_nifi.py --check    # regenerate in memory, diff, exit 1 on drift

One-way only: flow.json is the input, never the output. build_flow.py remains
the source of truth for the flow itself.

Two files come out because one file cannot do both jobs:

  pipeline.yml      WHAT the pipeline does, in vocabulary no engine owns.
                    This is the contract SSIS (or anything else) is graded on.
  engines/nifi.yml  HOW NiFi realizes each canonical stage.

The split of labour: everything mechanical is derived from flow.json, and the
one thing that is not -- which processor means which canonical stage -- lives in
mapping/nifi.map.yml. Any processor missing from that map is an error, so the
spec cannot silently drift behind the canvas.
"""
from __future__ import annotations

import argparse
import difflib
import json
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
FLOW = ROOT / "nifi" / "flow" / "ecommerce_etl.flow.json"
MAP = ROOT / "spec" / "mapping" / "nifi.map.yml"
OUT_PIPELINE = ROOT / "spec" / "pipeline.yml"
OUT_NIFI = ROOT / "spec" / "engines" / "nifi.yml"

# NiFi writes every property descriptor into the export, including the ones left
# at their default. Keeping them would bury the handful that carry meaning, so
# the binding drops anything that is pure NiFi housekeeping.
NOISE = {
    "Delete Attributes Expression", "Store State", "canonical-value-lookup-cache-size",
    "Stateful Variables Initial Value", "cache-schema", "dbf-default-precision",
    "dbf-default-scale", "table-schema-cache-size", "put-db-record-allow-multiple-statements",
    "put-db-record-schema-name", "put-db-record-field-containing-sql",
    "put-db-record-quoted-identifiers", "put-db-record-quoted-table-identifiers",
    "Support Fragmented Transactions", "Transaction Timeout", "Batch Size",
    "rollback-on-failure", "Obtain Generated Keys", "database-session-autocommit",
    "put-db-record-query-timeout", "put-db-record-binary-format",
    "put-db-record-max-batch-size", "put-db-record-translate-field-names",
    "Maximum File Age", "Minimum File Age", "Minimum File Size", "max-listing-time",
    "et-initial-listing-target", "et-time-window", "et-node-identifier",
    "Log level when file not found", "Log level when permission denied",
    "Owner", "Group", "Permissions", "Create Missing Directories", "Last Modified Time",
    "target-system-timestamp-precision", "track-performance", "max-performance-metrics",
    "max-operation-time", "listing-strategy", "Ignore Hidden Files",
    "Input Directory Location", "Recurse Subdirectories", "Include File Attributes",
    "include-zero-record-flowfiles", "record-path-lookup-miss-result-cache-size",
    "maximum-validation-details-length", "schema-text", "schema-access-strategy",
    "allow-extra-fields", "coerce-types", "result-contents", "record-update-strategy",
    "replacement-value-strategy",
}

# NiFi Expression Language, normalised into a small neutral function set so
# another engine can implement it without parsing NiFi syntax. See spec/README.md.
EL_RULES = [
    (re.compile(r"\$\{now\(\):toNumber\(\):minus\((\d+)\)\}"),
     lambda m: "{{ now_minus_days(%d) }}" % (int(m.group(1)) // 86_400_000)),
    (re.compile(r"\$\{now\(\):toNumber\(\):plus\((\d+)\)\}"),
     lambda m: "{{ now_plus_days(%d) }}" % (int(m.group(1)) // 86_400_000)),
    (re.compile(r"\$\{now\(\):toNumber\(\)\}"), lambda m: "{{ now() }}"),
    (re.compile(r"\$\{batch_id\}"), lambda m: "{{ batch_id }}"),
    (re.compile(r"\$\{source_file\}"), lambda m: "{{ source_file }}"),
    (re.compile(r"\$\{record\.count\}"), lambda m: "{{ record_count }}"),
    (re.compile(r"\$\{filename\}"), lambda m: "{{ source_file }}"),
]


def neutralise(text: str) -> str:
    """Rewrite NiFi Expression Language into the neutral function set."""
    if not isinstance(text, str):
        return text
    for pattern, repl in EL_RULES:
        text = pattern.sub(repl, text)
    return text


class Flow:
    """The exported flow, indexed for lookup by id and by name."""

    def __init__(self, path: Path):
        self.contents = json.loads(path.read_text())["flowContents"]
        self.processors = {p["name"]: p for p in self.contents["processors"]}
        self.services = {s["identifier"]: s for s in self.contents["controllerServices"]}
        self.names = {p["identifier"]: p["name"] for p in self.contents["processors"]}
        for i, funnel in enumerate(self.contents["funnels"], start=1):
            self.names[funnel["identifier"]] = f"FUNNEL{i}"
        self.edges = [
            (self.names.get(c["source"]["id"], "?"),
             tuple(c["selectedRelationships"]),
             self.names.get(c["destination"]["id"], "?"))
            for c in self.contents["connections"]
        ]

    def resolve(self, node: str, _seen: frozenset = frozenset()) -> str:
        """Funnels are pure plumbing. Follow one through to the real processor."""
        if not node.startswith("FUNNEL") or node in _seen:
            return node
        for src, _rel, dst in self.edges:
            if src == node:
                return self.resolve(dst, _seen | {node})
        return node

    def outputs(self, name: str) -> dict[str, str]:
        """relationship -> downstream processor, funnels collapsed."""
        out = {}
        for src, rels, dst in self.edges:
            if src != name:
                continue
            target = self.resolve(dst)
            for rel in rels or ("",):
                out[rel or "next"] = "(self — retry)" if target == name else target
        return out

    def clean_props(self, proc: dict) -> dict:
        """Meaningful properties only, with service ids swapped for service names."""
        props = {}
        for key, value in proc["properties"].items():
            if value is None or key in NOISE:
                continue
            if isinstance(value, str) and value in self.services:
                value = f"service:{self.services[value]['name']}"
            props[key] = neutralise(value)
        return props


def build_nifi_binding(flow: Flow, mapping: dict) -> dict:
    """engines/nifi.yml -- how NiFi realizes each canonical stage."""
    stages = {}
    for stage in mapping["stages"]:
        entries = []
        for name in stage["processors"]:
            proc = flow.processors[name]
            entry = {
                "name": name,
                "type": proc["type"],
                "properties": flow.clean_props(proc),
                "relationships": flow.outputs(name),
            }
            if proc.get("retriedRelationships"):
                entry["retry"] = {
                    "relationships": proc["retriedRelationships"],
                    "count": proc.get("retryCount"),
                    "backoff": proc.get("maxBackoffPeriod"),
                }
            entries.append(entry)
        stages[stage["id"]] = entries

    services = [
        {"name": s["name"],
         "type": s["type"],
         "properties": flow.clean_props(s)}
        for s in sorted(flow.contents["controllerServices"], key=lambda s: s["name"])
    ]

    return {
        "apiVersion": "engine-binding/v1",
        "engine": "nifi",
        "engine_version": "1.27.0",
        "implements": "pipeline-spec/v1",
        "extracted_from": str(FLOW.relative_to(ROOT)),
        "generated_by": "spec/extract_nifi.py — do not hand-edit",
        "runtime": {"controller_services": services},
        "stages": stages,
        "unmapped": mapping["unmapped"],
        "external_rejects": mapping["external_rejects"],
    }


def build_pipeline(flow: Flow, mapping: dict) -> dict:
    """pipeline.yml -- what the pipeline does, in nobody's vocabulary."""
    registry = next(s for s in flow.contents["controllerServices"]
                    if s["type"].endswith("AvroSchemaRegistry"))
    records = {}
    for key, raw in registry["properties"].items():
        if not isinstance(raw, str) or not raw.strip().startswith("{"):
            continue
        avro = json.loads(raw)
        records[key] = [
            {"name": f["name"],
             "type": (f["type"][1] if isinstance(f["type"], list) else f["type"]),
             "required": not isinstance(f["type"], list)}
            for f in avro["fields"]
        ]

    lookups, targets, alerts = {}, {}, []
    for name, proc in flow.processors.items():
        kind = proc["type"].rsplit(".", 1)[-1]
        props = proc["properties"]
        if kind == "LookupRecord":
            svc = flow.services[props["lookup-service"]]["properties"]
            lookups[name] = {
                "table": svc["dbrecord-lookup-table-name"],
                # the record path the lookup keys on, and the column it keys against
                "key": props["key"].lstrip("/"),
                "key_column": svc["dbrecord-lookup-key-column"],
                "returns": [c.strip() for c in
                            svc["dbrecord-lookup-value-columns"].split(",") if c.strip()],
            }
        elif kind == "PutDatabaseRecord":
            keys = props.get("put-db-record-update-keys")
            targets[props["put-db-record-table-name"]] = {
                "mode": props["put-db-record-statement-type"].lower(),
                **({"keys": [k.strip() for k in keys.split(",")]} if keys else {}),
            }
        elif kind == "QueryRecord":
            for rel, sql in props.items():
                if not isinstance(sql, str) or "FLOWFILE" not in sql.upper():
                    continue
                match = re.search(r"'(\w+)' AS alert_type.*?'(\w+)' AS severity", sql, re.S)
                if match:
                    cond = re.search(r"(?:HAVING|WHERE)\s+(.+?)$", sql.strip(), re.S)
                    alerts.append({
                        "name": match.group(1),
                        "severity": match.group(2),
                        "scope": "order" if "GROUP BY" in sql.upper() else "line",
                        "condition": neutralise(cond.group(1).strip()) if cond else None,
                    })

    stages = []
    for stage in mapping["stages"]:
        entry = {"id": stage["id"], "type": stage["type"], "describe": stage["describe"]}
        for name in stage["processors"]:
            proc = flow.processors[name]
            kind = proc["type"].rsplit(".", 1)[-1]
            props = proc["properties"]
            if kind == "ValidateRecord":
                entry["record"] = props["schema-name"]
                entry["strict_types"] = props.get("strict-type-checking") == "true"
            elif kind == "LookupRecord":
                entry["lookup"] = lookups[name]["table"]
                entry["on_key"] = lookups[name]["key"]
                entry["adds"] = lookups[name]["returns"]
            elif kind == "RouteOnAttribute":
                entry["condition"] = "file_size > 0"
            elif kind == "QueryRecord" and stage["type"] in ("filter", "aggregate", "project"):
                keep = {r: neutralise(s) for r, s in props.items()
                        if isinstance(s, str) and "FLOWFILE" in s.upper()
                        and "alert_type" not in s}
                if keep:
                    entry["sql"] = keep
            elif kind in ("ListFile", "PutFile"):
                path = props.get("Input Directory") or props.get("Directory")
                entry.setdefault("paths", {})[
                    "watch" if kind == "ListFile" else "dead_letter"] = path
            elif kind == "FetchFile":
                entry.setdefault("paths", {})["archive"] = props.get("Move Destination Directory")
            elif kind == "PutDatabaseRecord":
                entry.setdefault("writes", []).append(props["put-db-record-table-name"])
            elif kind == "UpdateAttribute" and "quarantine_reason" in props:
                entry.setdefault("reasons", []).append(props["quarantine_reason"])
        if "on_fail" in stage:
            entry["on_fail"] = stage["on_fail"]
        stages.append(entry)

    rejects = {
        proc["properties"]["quarantine_reason"]: _reason_doc(proc["properties"]["quarantine_reason"])
        for proc in flow.processors.values()
        if "quarantine_reason" in proc["properties"]
    }
    for reason in mapping["external_rejects"]:
        rejects[reason] = _reason_doc(reason)

    return {
        "apiVersion": "pipeline-spec/v1",
        "name": flow.contents["name"],
        "description": "Order-line ingestion: validate, enrich, apply rules, load, "
                       "and account for everything rejected on the way.",
        "generated_by": "spec/extract_nifi.py — do not hand-edit",
        "records": records,
        "lookups": {v["table"]: {"key": v["key"], "key_column": v["key_column"],
                                 "returns": v["returns"]}
                    for v in sorted(lookups.values(), key=lambda x: x["table"])},
        "stages": stages,
        "alerts": sorted(alerts, key=lambda a: a["name"]),
        "targets": dict(sorted(targets.items())),
        "rejects": dict(sorted(rejects.items())),
        "observability": {
            "job_ledger": {
                "table": "job_runs",
                "granularity": "one row per input file",
                "fields": ["batch_id", "source_file", "started_at", "finished_at",
                           "duration_ms", "records_valid", "records_invalid",
                           "orders_loaded", "status", "error_text"],
            },
            "dead_letter": {"path": "data/dlq", "keeps": "the original file, unmodified"},
        },
    }


def _reason_doc(reason: str) -> str:
    return {
        "SCHEMA_INVALID": "record did not match the order_line contract",
        "UNKNOWN_SKU": "sku is not present in the products table",
        "UNKNOWN_CUSTOMER": "customer_id is not present in the customers table",
        "RANGE_VIOLATION": "qty or unit_price is not a positive number",
        "BAD_CURRENCY": "currency is not one of INR / USD / EUR / GBP "
                        "(compared case-insensitively)",
        "BAD_TIMESTAMP": "order_ts is more than a year old or more than a day ahead",
        "READER_FAILURE": "input could not be parsed at all; whole file dead-lettered",
        "EMPTY_FILE": "input file was zero bytes",
    }.get(reason, reason)


class PlainDumper(yaml.SafeDumper):
    """No anchors/aliases. A repeated list must read as a repeated list here --
    `*id001` is correct YAML but hostile to a human and to naive consumers."""

    def ignore_aliases(self, data):
        return True


def dump(doc: dict) -> str:
    return yaml.dump(doc, Dumper=PlainDumper, sort_keys=False, width=100,
                     allow_unicode=True, indent=2)


def audit(flow: Flow, mapping: dict) -> list[str]:
    """Every processor must be accounted for. Silence here is the whole point."""
    mapped = {n for s in mapping["stages"] for n in s["processors"]}
    problems = []
    for missing in sorted(set(flow.processors) - mapped):
        problems.append(f"processor not in mapping/nifi.map.yml: {missing!r}")
    for ghost in sorted(mapped - set(flow.processors)):
        problems.append(f"mapping names a processor that is not in the flow: {ghost!r}")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true",
                    help="regenerate and diff against the committed files; exit 1 on drift")
    args = ap.parse_args()

    flow = Flow(FLOW)
    mapping = yaml.safe_load(MAP.read_text())

    problems = audit(flow, mapping)
    if problems:
        print("extraction refused — the flow and the mapping disagree:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 2

    outputs = {
        OUT_PIPELINE: dump(build_pipeline(flow, mapping)),
        OUT_NIFI: dump(build_nifi_binding(flow, mapping)),
    }

    if args.check:
        drift = False
        for path, text in outputs.items():
            current = path.read_text() if path.exists() else ""
            if current != text:
                drift = True
                print(f"DRIFT  {path.relative_to(ROOT)}")
                sys.stdout.writelines(difflib.unified_diff(
                    current.splitlines(True), text.splitlines(True),
                    fromfile="committed", tofile="regenerated"))
        if drift:
            print("\nre-run without --check to update.", file=sys.stderr)
            return 1
        print(f"up to date — {len(flow.processors)} processors, "
              f"{len(flow.edges)} connections accounted for")
        return 0

    for path, text in outputs.items():
        path.write_text(text)
        print(f"wrote {path.relative_to(ROOT)}  ({len(text.splitlines())} lines)")
    print(f"{len(flow.processors)} processors, {len(flow.edges)} connections accounted for")
    return 0


if __name__ == "__main__":
    sys.exit(main())
