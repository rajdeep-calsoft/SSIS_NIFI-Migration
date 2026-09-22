"""Writes <pkg>.mapping.json alongside a generated flow.json: which columns
this package's own OLEDBDestination components declared for each target
table, read straight from the IR's derived.columns (see
ssis2nifi/catalog/derive.py::_oledb_destination) -- never invented here.

Why this exists: the report engine (report/introspect.py) needs to know
which destination table(s) a job actually populates before it can introspect
their primary key / column list via information_schema. This sidecar is that
handoff, produced once at convert time from facts the parser already
extracted, so the report never has to re-parse the .dtsx itself.

Column-name matching is documented, not invented: the reference repo's own
parser/derive/emit chain -- and the NiFi runtime config the catalogue recipes
emit (`put-db-record-translate-field-names: true`) -- already assume a
source column lands under the SAME name in the destination table. This
sidecar records that assumption explicitly (one row per column) rather than
building a renaming resolver nothing in the pipeline actually uses yet.
"""
from __future__ import annotations

import json
from pathlib import Path

from ssis2nifi.ir.model import Package

OLEDB_DESTINATION_CLASS_IDS = (
    "Microsoft.OLEDBDestination",
    "{4ADA7EAA-136C-4215-8098-D7A7C27FC0D1}",
)


def build(pkg: Package) -> dict:
    """{table_qualified: {schema, table, columns: [...]}} for every OLEDB
    destination this package writes to."""
    tables: dict[str, dict] = {}
    for df in pkg.dataflows:
        for comp in df.components:
            if comp.class_id not in OLEDB_DESTINATION_CLASS_IDS:
                continue
            d = comp.derived or {}
            table = d.get("target_qualified")
            if not table:
                continue
            tables[table] = {
                "schema": d.get("target_schema"),
                "table": d.get("target_table"),
                "columns": d.get("columns", []),
                "source_component": comp.name,
            }
    return {"package": pkg.name, "destinations": tables}


def write(pkg: Package, flow_path: Path) -> Path:
    out = flow_path.with_suffix("").with_suffix(".mapping.json")
    out.write_text(json.dumps(build(pkg), indent=2))
    return out
