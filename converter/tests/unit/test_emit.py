"""Generating a flow: determinism, wiring, and the two safety properties.

These run offline. That is the point of emitting a file rather than making REST
calls -- generation is a pure function, so the expensive half of the test suite
needs no NiFi, no database and no Docker.

The live import check (`ssis2nifi verify`) is a separate, slower gate.
"""

from __future__ import annotations

import json

import pytest

from ssis2nifi.emit import flowdef

from .conftest import CORPUS, ROOT, converted

BINDINGS = ROOT / "bindings" / "L1.bindings.yml"


@pytest.fixture(scope="session")
def flow():
    return converted(CORPUS / "L1.dtsx", BINDINGS)


# --- determinism ---------------------------------------------------------

def test_generation_is_byte_identical_across_runs():
    """uuid5, not uuid4. Golden-file testing depends on this."""
    a, _, _ = converted(CORPUS / "L1.dtsx", BINDINGS)
    b, _, _ = converted(CORPUS / "L1.dtsx", BINDINGS)
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


def test_identifiers_trace_back_to_the_ssis_component(flow):
    """Provenance: a processor on the canvas says which refId produced it."""
    doc, _, _ = flow
    for proc in doc["flowContents"]["processors"]:
        if proc["name"] in ("Rejected rows", "Make reject filename unique"):
            continue                       # synthesised, has no SSIS origin
        if proc["name"] == "Stamp batch id" or proc["name"].startswith(("Stamp batch id (", "Record ")):
            continue                       # job-ledger infra, no SSIS equivalent
        if proc["name"] in ("Detect Alerts", "Load Alerts"):
            continue                       # alerts infra, no SSIS equivalent
        assert "Package\\" in proc["comments"], f"{proc['name']} has no source refId"
        assert "rule:" in proc["comments"]


# --- wiring --------------------------------------------------------------

def test_the_happy_path_is_wired_in_order(flow):
    doc, _, _ = flow
    conns = {
        (c["source"]["name"], tuple(c["selectedRelationships"]), c["destination"]["name"])
        for c in doc["flowContents"]["connections"]
    }
    # The source's success now fans out through the job-ledger's batch-id
    # stamp before reaching the real first step -- see flowdef._add_job_ledger.
    assert ("Extract Sample Currency Data", ("success",), "Stamp batch id") in conns
    assert ("Stamp batch id", ("success",), "Lookup Currency Key") in conns
    # SSIS's "Lookup Match Output" must become NiFi's "matched", not "success"
    assert ("Lookup Currency Key", ("matched",), "Lookup Date Key") in conns
    assert ("Lookup Date Key", ("matched",),
            "Stamp batch id (NewFactCurrencyRate)") in conns
    assert ("Stamp batch id (NewFactCurrencyRate)", ("success",),
            "Sample OLE DB Destination") in conns


def test_one_connection_pool_is_shared_not_one_per_component(flow):
    """Both lookups and the destination hit the same database."""
    doc, _, _ = flow
    pools = [s for s in doc["flowContents"]["controllerServices"]
             if s["type"].endswith("DBCPConnectionPool")]
    assert len(pools) == 1


def test_each_lookup_gets_its_own_lookup_service(flow):
    doc, _, _ = flow
    services = [s for s in doc["flowContents"]["controllerServices"]
                if s["type"].endswith("DatabaseRecordLookupService")]
    assert len(services) == 2
    # Lower-cased because the binding declares identifier_case: lower --
    # Postgres folds unquoted identifiers, so dbo.DimDate would be "not found".
    tables = {s["properties"]["dbrecord-lookup-table-name"] for s in services}
    assert tables == {"dbo.dimcurrency", "dbo.dimdate"}


def test_lookup_key_column_comes_from_the_per_column_property(flow):
    """JoinToReferenceColumn lives on the input COLUMN, not the component.

    The property is `dbrecord-lookup-key-column`. Both it and
    `dbrecord-lookup-lookup-key-column` exist on the service, but only this one
    is required -- the other is rejected as "not a supported property" and the
    service never enables. Verified against a live NiFi 1.27.0's descriptors.
    """
    doc, _, _ = flow
    services = {
        s["properties"]["dbrecord-lookup-table-name"]:
            s["properties"]["dbrecord-lookup-key-column"]
        for s in doc["flowContents"]["controllerServices"]
        if s["type"].endswith("DatabaseRecordLookupService")
    }
    assert services["dbo.dimcurrency"] == "currencyalternatekey"
    assert services["dbo.dimdate"] == "fulldatealternatekey"


def test_downstream_processors_read_json_not_the_source_format(flow):
    """A record processor reads what its upstream neighbour WROTE.

    Only the first processor in a chain sees the source file's format; every
    one after it sees the record writer's JSON. Giving them all the CSV reader
    makes the second one fail with MalformedRecordException.
    """
    doc, _, _ = flow
    fc = doc["flowContents"]
    by_id = {p["identifier"]: p for p in fc["processors"]}
    svc = {s["identifier"]: s for s in fc["controllerServices"]}
    upstream = {c["destination"]["id"]: c["source"]["id"] for c in fc["connections"]}

    for proc in fc["processors"]:
        reader_id = proc["properties"].get("record-reader") or \
                    proc["properties"].get("put-db-record-record-reader")
        if not reader_id:
            continue
        src = by_id.get(upstream.get(proc["identifier"], ""), {})
        expected_csv = src.get("type", "").endswith("GetFile")
        is_csv = svc[reader_id]["type"].endswith("CSVReader")
        assert is_csv == expected_csv, (
            f"{proc['name']} reads from {src.get('name')} but uses "
            f"{svc[reader_id]['type'].rsplit('.', 1)[-1]}"
        )


def test_identifiers_are_folded_for_the_target_dialect(flow):
    """Postgres folds unquoted identifiers; SQL Server does not care.

    `[dbo].[NewFactCurrencyRate]` from the .dtsx is a different -- missing --
    table on Postgres unless it is folded, and the flow deploys cleanly before
    failing at runtime with "table not found".
    """
    doc, _, _ = flow
    dest = next(p for p in doc["flowContents"]["processors"]
                if p["type"].endswith("PutDatabaseRecord"))
    assert dest["properties"]["put-db-record-table-name"] == "newfactcurrencyrate"
    assert dest["properties"]["put-db-record-schema-name"] == "dbo"


# --- safety property 1: no silent row loss -------------------------------

def test_a_fail_component_no_match_output_is_routed_not_dropped(flow):
    """The trap. NoMatchBehavior=0 means SSIS fails the data flow on a miss.

    Auto-terminating `unmatched` would discard those rows instead: identical
    row counts on the happy path, opposite behaviour when it matters.

    Both misses route through "Make reject filename unique" first, not
    straight to "Rejected rows" -- see the next test for why.
    """
    doc, _, _ = flow
    unmatched = [
        c for c in doc["flowContents"]["connections"]
        if "unmatched" in c["selectedRelationships"]
    ]
    assert len(unmatched) == 2, "both lookups must route their misses somewhere"
    assert all(c["destination"]["name"] == "Make reject filename unique" for c in unmatched)

    for proc in doc["flowContents"]["processors"]:
        if proc["type"].endswith("LookupRecord"):
            assert "unmatched" not in proc["autoTerminatedRelationships"]


def test_every_error_output_reaches_the_reject_sink(flow):
    """Every dangling output reaches the sink -- through the filename stamp.

    Two different components can both be missing on the SAME source file
    (e.g. both lookups) and, without the stamp, both write PutFile's
    Directory/${filename} with "replace" conflict resolution -- the second
    write silently erases the first's rows, with no error anywhere. Found by
    `make verify-behavior`, not by reading the property descriptors.
    """
    doc, _, _ = flow
    fc = doc["flowContents"]
    into_stamp = {c["source"]["name"] for c in fc["connections"]
                  if c["destination"]["name"] == "Make reject filename unique"}
    assert into_stamp == {
        "Extract Sample Currency Data", "Lookup Currency Key",
        "Lookup Date Key", "Sample OLE DB Destination",
    }

    stamp_to_sink = [c for c in fc["connections"]
                      if c["source"]["name"] == "Make reject filename unique"]
    assert len(stamp_to_sink) == 1
    assert stamp_to_sink[0]["destination"]["name"] == "Rejected rows"
    assert stamp_to_sink[0]["selectedRelationships"] == ["success"]

    stamp = next(p for p in fc["processors"] if p["name"] == "Make reject filename unique")
    assert stamp["properties"]["filename"] == "${filename}-${uuid}"


def test_every_relationship_is_connected_or_auto_terminated(flow):
    """NiFi marks a processor invalid otherwise. This is the import gate,
    asserted offline so it fails in milliseconds rather than on a canvas."""
    doc, _, _ = flow
    bundles, _ = flowdef.load_catalogue()
    declared = bundles["relationships"]

    connected: dict[str, set[str]] = {}
    for conn in doc["flowContents"]["connections"]:
        connected.setdefault(conn["source"]["id"], set()).update(conn["selectedRelationships"])

    for proc in doc["flowContents"]["processors"]:
        kind = proc["type"].rsplit(".", 1)[-1]
        rels = declared.get(kind)
        if isinstance(rels, dict):
            rels = rels.get(proc["properties"].get("routing-strategy", "route-to-success"), [])
        if not rels:
            continue
        handled = set(proc["autoTerminatedRelationships"]) | connected.get(proc["identifier"], set())
        assert not set(rels) - handled, f"{proc['name']}: {set(rels) - handled} unhandled"


# --- safety property 2: no secrets in the artifact -----------------------

def test_the_password_is_null_in_the_flow_and_named_in_the_sidecar(flow):
    doc, secrets, _ = flow
    pool = next(s for s in doc["flowContents"]["controllerServices"]
                if s["type"].endswith("DBCPConnectionPool"))
    assert pool["properties"]["Password"] is None

    assert len(secrets) == 1
    assert secrets[0]["env_var"] == "SSIS2NIFI_DB_MAIN_PASSWORD"


def test_no_secret_value_appears_anywhere_in_the_artifact(flow):
    doc, _, _ = flow
    blob = json.dumps(doc)
    for forbidden in ("password", "PWD=", "secret"):
        # property NAMES may contain these; VALUES must not carry a credential
        assert f'"{forbidden}"' not in blob.replace('"Password"', "")


# --- refusing rather than emitting something broken -----------------------

def test_a_missing_binding_is_refused_with_a_useful_message():
    from ssis2nifi.catalog.derive import derive
    from ssis2nifi.catalog.support import annotate
    from ssis2nifi.dtsx.parse import parse_file

    pkg = derive(annotate(parse_file(str(CORPUS / "L1.dtsx"))))
    with pytest.raises(flowdef.EmitError) as exc:
        flowdef.build(pkg, {})
    assert "binding" in str(exc.value).lower()


def test_components_without_a_recipe_are_skipped_and_reported():
    """L4's 'Failed Rows' is a Microsoft.FlatFileDestination -- SUPPORTED (see
    catalog/support.py) but with no catalogue/components/*.yml recipe written
    for it yet. It must not silently become a processor."""
    doc, _, notes = converted(CORPUS / "L4.dtsx", BINDINGS)
    names = {p["name"] for p in doc["flowContents"]["processors"]}
    assert "Failed Rows" not in names
    assert any("no recipe" in n for n in notes)


def test_script_component_get_error_description_idiom_converts():
    """L4's Script Component ('Get Error Description') matches the one
    recognised idiom (catalog/script_pattern.py) and DOES become a real
    LookupRecord processor -- narrowing this from a blanket refusal is the
    whole point of catalogue/components/microsoft.managedcomponenthost.yml."""
    doc, _, notes = converted(CORPUS / "L4.dtsx", BINDINGS)
    names = {p["name"] for p in doc["flowContents"]["processors"]}
    assert "Get Error Description" in names
    assert any("SCRIPT_ERROR_CATALOGUE_PARTIAL" in n for n in notes)
