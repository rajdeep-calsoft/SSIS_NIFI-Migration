"""Author the whole ETL flow through the NiFi REST API.

Why code and not clicking: a canvas built by hand exists only in one person's
NiFi instance. This script is the source of truth -- it runs against an empty
canvas, produces the process group, and `export-flow` then dumps the result to
nifi/flow/ecommerce_etl.flow.json, which is what ships and what boots.

Flow shape (left to right on the canvas):

  ListFile -> FetchFile -> stamp batch id -> ValidateRecord
      -> LookupRecord(products)  -> LookupRecord(customers)
      -> QueryRecord(business rules)
            |-- clean ---> QueryRecord(aggregate orders + alert rules) -> orders / alerts
            |         \\-> QueryRecord(project line items)             -> order_items
            \\- range_violation / bad_currency / bad_timestamp -> quarantine

  Every reject path converges on one funnel, gets a reason stamped on it, and
  lands in quarantine_records. Every failure path converges on a DLQ funnel.

  Six reject reasons, one per rule that can turn a record away:
    SCHEMA_INVALID  UNKNOWN_SKU  UNKNOWN_CUSTOMER
    RANGE_VIOLATION  BAD_CURRENCY  BAD_TIMESTAMP
  A seventh, READER_FAILURE, is raised outside NiFi by the DLQ sweeper in
  monitor.py. The names match the SSIS implementation so the two engines'
  reject tables can be compared directly.
"""
from __future__ import annotations

import json
import os

from .nifi_api import NiFi

GROUP_NAME = "ecommerce_etl"

DATA_DIR = "/opt/nifi/data"
TS_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

# The validation contract. Deliberately plain Avro types (no logical types):
# records travel through the flow with their natural inferred types and the
# only conversion happens at the JDBC boundary, which is where it is reliable.
ORDER_LINE_SCHEMA = {
    "type": "record",
    "name": "order_line",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "line_no", "type": "int"},
        {"name": "customer_id", "type": "string"},
        {"name": "order_ts", "type": "long"},
        {"name": "order_ts_iso", "type": ["null", "string"], "default": None},
        {"name": "status", "type": "string"},
        {"name": "channel", "type": "string"},
        {"name": "payment_type", "type": ["null", "string"], "default": None},
        {"name": "currency", "type": "string"},
        {"name": "sku", "type": "string"},
        {"name": "qty", "type": "int"},
        {"name": "unit_price", "type": "double"},
        {"name": "line_total", "type": "double"},
    ],
}

# Rejected records are heterogeneous by nature: qty is 3 in one row and "N/A"
# in the next. Schema inference turns that into a CHOICE type, and
# PutDatabaseRecord cannot bind a CHOICE to a column ("Cannot convert CHOICE,
# type must be explicit"). Writing rejects through an explicit all-strings
# schema collapses the ambiguity before it ever reaches the database.
QUARANTINE_SCHEMA = {
    "type": "record",
    "name": "quarantine_record",
    "fields": [
        {"name": n, "type": ["null", "string"], "default": None}
        for n in ("reason", "detail", "batch_id", "source_file", "order_id",
                  "line_no", "customer_id", "order_ts", "sku", "qty",
                  "unit_price", "line_total", "currency", "status", "channel")
    ],
}

P = "org.apache.nifi.processors.standard."

# Almost everything we use lives in nifi-standard-nar; UpdateAttribute does not.
TYPE_OVERRIDES = {
    "UpdateAttribute": "org.apache.nifi.processors.attributes.UpdateAttribute",
}


def ptype_of(short_name: str) -> str:
    return TYPE_OVERRIDES.get(short_name, P + short_name)


def run() -> int:
    nifi = NiFi()
    nifi.wait_until_ready()

    high_value = os.getenv("HIGH_VALUE_THRESHOLD", "150000")
    susp_qty = os.getenv("SUSPICIOUS_QTY", "50")
    pg_url = (f"jdbc:postgresql://{os.getenv('PGHOST', 'postgres')}:"
              f"{os.getenv('PGPORT', '5432')}/{os.getenv('PGDATABASE', 'etldemo')}")

    root = nifi.root_id()

    existing = nifi.find_child_group(root, GROUP_NAME)
    if existing:
        print(f"[build] group '{GROUP_NAME}' already exists -- removing it first")
        nifi.delete_group(existing["id"])

    pg = nifi.create_process_group(root, GROUP_NAME, (100, 100))
    gid = pg["id"]
    print(f"[build] created process group {GROUP_NAME} ({gid})")

    # ------------------------------------------------------------------
    # Controller services
    # ------------------------------------------------------------------
    registry = nifi.create_controller_service(
        gid, "org.apache.nifi.schemaregistry.services.AvroSchemaRegistry",
        "OrderSchemas", {"order_line": json.dumps(ORDER_LINE_SCHEMA),
                         "quarantine_record": json.dumps(QUARANTINE_SCHEMA)})["id"]

    # Reader infers types from the JSON itself. This is deliberate: an explicit
    # schema on the READER would make it throw on qty:"two" and take the whole
    # batch down, instead of letting ValidateRecord reject that one record.
    # NOTE: deliberately NO "Timestamp Format" here. Setting it makes schema
    # INFERENCE treat any ISO-8601 looking string (order_ts_iso) as a TIMESTAMP,
    # which then fails strict validation against a declared "string" field.
    # Timestamps travel as epoch millis, so the reader never needs to parse one.
    reader = nifi.create_controller_service(
        gid, "org.apache.nifi.json.JsonTreeReader", "InferReader",
        {"schema-access-strategy": "infer-schema"})["id"]

    writer = nifi.create_controller_service(
        gid, "org.apache.nifi.json.JsonRecordSetWriter", "JsonWriter",
        {"Schema Write Strategy": "no-schema",
         "schema-access-strategy": "inherit-record-schema",
         "Timestamp Format": TS_FMT})["id"]

    quarantine_writer = nifi.create_controller_service(
        gid, "org.apache.nifi.json.JsonRecordSetWriter", "QuarantineWriter",
        {"Schema Write Strategy": "no-schema",
         "schema-access-strategy": "schema-name",
         "schema-registry": registry,
         "schema-name": "quarantine_record",
         # drop nulls so a field that is null in every record does not infer
         # as a null-typed column downstream
         "suppress-nulls": "always-suppress"})["id"]

    pool = nifi.create_controller_service(
        gid, "org.apache.nifi.dbcp.DBCPConnectionPool", "PostgresPool",
        {"Database Connection URL": pg_url,
         "Database Driver Class Name": "org.postgresql.Driver",
         "database-driver-locations": "/opt/nifi/drivers",
         "Database User": os.getenv("PGUSER", "etl"),
         "Password": os.getenv("PGPASSWORD", "etlpass"),
         "Max Total Connections": "12"})["id"]

    product_lookup = nifi.create_controller_service(
        gid, "org.apache.nifi.lookup.db.DatabaseRecordLookupService", "ProductLookup",
        {"dbrecord-lookup-dbcp-service": pool,
         "dbrecord-lookup-table-name": "products",
         "dbrecord-lookup-key-column": "sku",
         "dbrecord-lookup-value-columns": "category",
         "dbrecord-lookup-cache-size": "500",
         "Cache Expiration": "5 mins"})["id"]

    customer_lookup = nifi.create_controller_service(
        gid, "org.apache.nifi.lookup.db.DatabaseRecordLookupService", "CustomerLookup",
        {"dbrecord-lookup-dbcp-service": pool,
         "dbrecord-lookup-table-name": "customers",
         "dbrecord-lookup-key-column": "customer_id",
         "dbrecord-lookup-value-columns": "country,segment",
         "dbrecord-lookup-cache-size": "1000",
         "Cache Expiration": "5 mins"})["id"]

    # The replay guard. Cache DISABLED on purpose: a line loaded one file ago
    # must be visible to the very next lookup, and a cached miss would let the
    # replay through -- which is the exact bug this processor exists to catch.
    # The cost is one query per record; the SSIS engine pays the same cost as
    # one full read of clean_sales per file.
    replay_lookup = nifi.create_controller_service(
        gid, "org.apache.nifi.lookup.db.DatabaseRecordLookupService", "ReplayLookup",
        {"dbrecord-lookup-dbcp-service": pool,
         "dbrecord-lookup-table-name": "v_loaded_line_keys",
         "dbrecord-lookup-key-column": "order_line_id",
         "dbrecord-lookup-value-columns": "already_loaded",
         "dbrecord-lookup-cache-size": "0"})["id"]

    print("[build] controller services created")

    # ------------------------------------------------------------------
    # Processors
    # ------------------------------------------------------------------
    def proc(ptype, name, pos, props=None, auto_term=None, period=None,
             retry_rels=None):
        config = {"properties": props or {}}
        if auto_term:
            config["autoTerminatedRelationships"] = auto_term
        if period:
            config["schedulingPeriod"] = period
        if retry_rels:
            # Transient infrastructure failures (a DB blip) must be retried in
            # place, not dead-lettered. Without this, stopping Postgres for 20s
            # dumps every in-flight batch of GOOD data into the DLQ.
            config["retriedRelationships"] = retry_rels
            config["retryCount"] = 5
            config["backoffMechanism"] = "PENALIZE_FLOWFILE"
            config["maxBackoffPeriod"] = "30 secs"
        return nifi.create_processor(gid, ptype_of(ptype), name, pos, config)["id"]

    x, y = 0, 0
    COL = 420
    ROW = 190

    list_file = proc("ListFile", "1. List landing files", (x, y), {
        "Input Directory": f"{DATA_DIR}/landing",
        "File Filter": r".*\.json",
        "Recurse Subdirectories": "false",
        # a file must sit still for 2s before we touch it -- belt and braces
        # alongside the generator's atomic rename
        "Minimum File Age": "2 sec",
    }, period="5 sec")

    fetch_file = proc("FetchFile", "2. Fetch file", (x + COL, y), {
        "File to Fetch": "${absolute.path}/${filename}",
        "Completion Strategy": "Move File",
        "Move Destination Directory": f"{DATA_DIR}/archive",
        "Move Conflict Strategy": "Rename",
    }, auto_term=["not.found", "permission.denied"])

    stamp = proc("UpdateAttribute", "3. Stamp batch id", (x + 2 * COL, y), {
        "batch_id": "${filename:substringBeforeLast('.')}",
        "source_file": "${filename}",
        "ingest_start": "${now():toNumber()}",
    })

    # A zero-byte file otherwise disappears without trace: ListFile lists it,
    # the reader finds no records, and every downstream processor drops an empty
    # record set. For a pipeline whose job is monitoring, a file that arrives and
    # produces nothing must still leave a row behind.
    empty_guard = proc("RouteOnAttribute", "3b. Guard: empty file",
                       (x + 2 * COL + 200, y + 2 * ROW), {
        "Routing Strategy": "Route to Property name",
        "empty": "${fileSize:lt(1)}",
    })

    # The SSIS side separates "the field is not there" (NULL_CRITICAL) from
    # "the field is there but unreadable" (PARSE_ERROR). ValidateRecord alone
    # cannot tell those apart -- both just fail the schema -- so the null case
    # is taken out in front of it. What ValidateRecord rejects afterwards is,
    # by construction, present-but-unparseable.
    #
    # Mirrors stream_runner.py:171-176, including its `x in (None, "")`: an
    # empty string counts as absent, NOT as a parse failure.
    required = ["order_id", "sku", "customer_id", "qty", "unit_price", "order_ts"]
    missing_any = " OR ".join(
        f"{f} IS NULL OR CAST({f} AS VARCHAR) = ''" for f in required)

    require_fields = proc("QueryRecord", "3c. Required fields",
                          (x + 2 * COL + 260, y), {
        "record-reader": reader,
        "record-writer": writer,
        "present": f"SELECT * FROM FLOWFILE WHERE NOT ({missing_any})",
        "null_critical": f"SELECT * FROM FLOWFILE WHERE {missing_any}",
        "include-zero-record-flowfiles": "false",
    }, auto_term=["original"])

    validate = proc("ValidateRecord", "4. Validate against schema", (x + 3 * COL, y), {
        "record-reader": reader,
        "record-writer": writer,
        "schema-access-strategy": "schema-name-property",
        "schema-registry": registry,
        "schema-name": "order_line",
        "allow-extra-fields": "true",
        "strict-type-checking": "true",
        # coercion off on purpose: records keep their natural types all the way
        # to PutDatabaseRecord, which converts once, against the real column type
        "coerce-types": "false",
        "validation-details-attribute-name": "validation.details",
    })

    lookup_sku = proc("LookupRecord", "5. Enrich category / check SKU", (x + 4 * COL, y), {
        "record-reader": reader,
        "record-writer": writer,
        "lookup-service": product_lookup,
        "routing-strategy": "route-to-matched-unmatched",
        "result-contents": "record-fields",
        "result-record-path": "/",
        "key": "/sku",
    }, retry_rels=["failure"])

    lookup_cust = proc("LookupRecord", "6. Enrich country / check customer",
                       (x + 5 * COL, y), {
        "record-reader": reader,
        "record-writer": writer,
        "lookup-service": customer_lookup,
        "routing-strategy": "route-to-matched-unmatched",
        "result-contents": "record-fields",
        "result-record-path": "/",
        "key": "/customer_id",
    }, retry_rels=["failure"])

    # Business rules. Anything failing them is data that is structurally fine
    # but commercially nonsense -- exactly the class of error a schema misses.
    #
    # Each rule gets its OWN reject relationship rather than one shared
    # "rejected". Knowing a record failed is worth much less than knowing which
    # rule it failed, and the SSIS side reports at this granularity too, so a
    # single BAD_VALUES bucket could not be compared against it.
    #
    # UPPER() on currency: 'inr' is the same currency as 'INR'. Case is a
    # formatting difference, not a data error.
    # Bounds and ordering both come from the SSIS side's _validate_stream
    # (stream_runner.py:177-207), which this pipeline is being matched against.
    #   qty         1 .. 999          (was: qty > 0)
    #   unit_price  0.01 .. 9999.99   (was: unit_price > 0)
    #   order_ts    +/- 365 days      (was: -365 days .. +1 day, asymmetric)
    # The window is symmetric there, so a future-dated order is judged the same
    # way by both engines.
    amounts_ok = ("qty >= 1 AND qty <= 999 "
                  "AND unit_price >= 0.01 AND unit_price <= 9999.99")
    currency_ok = "UPPER(currency) IN ('INR','USD','EUR','GBP')"
    ts_ok = ("order_ts BETWEEN ${now():toNumber():minus(31536000000)} "
             "AND ${now():toNumber():plus(31536000000)}")
    # His ladder tests the timestamp BEFORE the currency. A record that breaks
    # both is BAD_TIMESTAMP there, so it must be BAD_TIMESTAMP here too.
    sane = f"{amounts_ok} AND {ts_ok} AND {currency_ok}"

    # The three reject queries are deliberately mutually exclusive -- each one
    # re-asserts the earlier rules as passing. Without that a record breaking
    # two rules would be emitted twice and quarantined twice, inflating the
    # reject count and breaking the comparison against SSIS.
    rules = proc("QueryRecord", "7. Business rules", (x + 6 * COL, y), {
        "record-reader": reader,
        "record-writer": writer,
        "clean": f"SELECT * FROM FLOWFILE WHERE {sane}",
        "range_violation": f"SELECT * FROM FLOWFILE WHERE NOT ({amounts_ok})",
        "bad_timestamp":
            f"SELECT * FROM FLOWFILE WHERE {amounts_ok} AND NOT ({ts_ok})",
        "bad_currency":
            f"SELECT * FROM FLOWFILE WHERE {amounts_ok} AND {ts_ok} "
            f"AND NOT ({currency_ok})",
        "include-zero-record-flowfiles": "false",
    }, auto_term=["original"])

    orders_q = proc("QueryRecord", "8. Aggregate orders + alert rules",
                    (x + 7 * COL, y - ROW), {
        "record-reader": reader,
        "record-writer": writer,
        "orders": (
            "SELECT order_id,"
            " MAX(customer_id) AS customer_id,"
            " MAX(order_ts) AS order_ts,"
            " MAX(status) AS status,"
            " MAX(channel) AS channel,"
            " MAX(payment_type) AS payment_type,"
            " MAX(currency) AS currency,"
            " MAX(country) AS country,"
            " MAX(segment) AS segment,"
            " SUM(line_total) AS order_total,"
            " SUM(qty) AS item_count,"
            " '${batch_id}' AS batch_id,"
            " '${source_file}' AS source_file"
            " FROM FLOWFILE GROUP BY order_id"
        ),
        # Per LINE, not per order. The SSIS side raises HIGH_VALUE on
        # `line_total > 500` for a single line (stream_runner.py:217), so
        # grouping by order here would alert on a different set of records.
        #
        # It also tests the line_total that ARRIVED, not qty * unit_price
        # recomputed -- matched deliberately, bug for bug, because an order
        # with a missing line_total raises no alert there either.
        "high_value": (
            "SELECT 'HIGH_VALUE' AS alert_type,"
            " 'WARN' AS severity,"
            " order_id, customer_id,"
            " CAST(line_total AS DOUBLE) AS metric_value,"
            " 'line total above threshold' AS detail,"
            " '${batch_id}' AS batch_id"
            f" FROM FLOWFILE WHERE line_total > {high_value}"
        ),
        "suspicious": (
            "SELECT 'SUSPICIOUS_QTY' AS alert_type,"
            " 'CRITICAL' AS severity,"
            " order_id, customer_id,"
            " CAST(qty AS DOUBLE) AS metric_value,"
            " 'line quantity far above normal' AS detail,"
            " '${batch_id}' AS batch_id"
            f" FROM FLOWFILE WHERE qty > {susp_qty}"
        ),
        "include-zero-record-flowfiles": "false",
    }, auto_term=["original"])

    # Dedupe on the business key (order_id, line_no), the way the SSIS side
    # does (stream_runner.py:288-306). Two cases, and only the first is
    # visible to a single QueryRecord:
    #
    #   within this file  -- the same line appears twice in one batch. The
    #     first occurrence wins; later ones become DUP_KEY.
    #   across files      -- a replayed batch. Caught by the lookup below,
    #     which asks the warehouse whether the key is already loaded.
    #
    # Both matter: this pipeline's UPSERT keeps the fact tables correct either
    # way, but "correct and silent" is not the same as "reported", and a replay
    # that nobody records is a replay nobody can investigate.
    dedupe = proc("QueryRecord", "8b. Dedupe on (order_id, line_no)",
                  (x + 6 * COL + 260, y), {
        "record-reader": reader,
        "record-writer": writer,
        # order_line_id is built here rather than in a processor of its own:
        # `8c. Check replay` needs a single-column key to look up, and this
        # query is already touching every field.
        "unique": (
            "SELECT t.*, t.order_id || '#' || CAST(t.line_no AS VARCHAR)"
            " AS order_line_id"
            " FROM (SELECT FLOWFILE.*, ROW_NUMBER() OVER ("
            " PARTITION BY order_id, line_no ORDER BY order_id) AS dup_rank"
            " FROM FLOWFILE) t WHERE t.dup_rank = 1"
        ),
        "dup_key": (
            "SELECT * FROM (SELECT FLOWFILE.*, ROW_NUMBER() OVER ("
            " PARTITION BY order_id, line_no ORDER BY order_id) AS dup_rank"
            " FROM FLOWFILE) WHERE dup_rank > 1"
        ),
        "include-zero-record-flowfiles": "false",
    }, auto_term=["original"])

    # Rule 14, the cross-BATCH half. `8b` only sees inside one file; his
    # engine builds its dedupe set from everything already cleaned, so a line
    # replayed in a later file is DUP_KEY there and was silently UPSERTed here.
    # Measured before this existed: a repeated 15-order file produced 28
    # DUP_KEY rejects on his side and 0 on this one.
    #
    # The UPSERT stays as the second line of defence -- this makes the replay
    # *reported*, it does not make the tables depend on it.
    replay_check = proc("LookupRecord", "8c. Check replay (order_id, line_no)",
                        (x + 6 * COL + 520, y), {
        "record-reader": reader,
        "record-writer": writer,
        "lookup-service": replay_lookup,
        "routing-strategy": "route-to-matched-unmatched",
        "result-contents": "record-fields",
        "result-record-path": "/",
        "key": "/order_line_id",
    }, retry_rels=["failure"])

    items_q = proc("QueryRecord", "9. Project line items", (x + 7 * COL, y + ROW), {
        "record-reader": reader,
        "record-writer": writer,
        "items": (
            "SELECT order_id, line_no, sku, qty, unit_price, line_total, category,"
            " '${batch_id}' AS batch_id FROM FLOWFILE"
        ),
        "include-zero-record-flowfiles": "false",
    }, auto_term=["original"])

    def put_db(name, pos, table, statement="INSERT", keys=None):
        props = {
            "put-db-record-record-reader": reader,
            "db-type": "PostgreSQL",
            "put-db-record-statement-type": statement,
            "put-db-record-dcbp-service": pool,
            "put-db-record-table-name": table,
            # heterogeneous reject shapes and enrichment extras both need this
            "put-db-record-unmatched-field-behavior": "Ignore Unmatched Fields",
            "put-db-record-unmatched-column-behavior": "Ignore Unmatched Columns",
            "put-db-record-max-batch-size": "500",
        }
        if keys:
            props["put-db-record-update-keys"] = keys
        return proc("PutDatabaseRecord", name, pos, props,
                    retry_rels=["failure"])

    put_orders = put_db("10. Load orders", (x + 8 * COL, y - ROW), "orders",
                        "UPSERT", "order_id")
    put_items = put_db("11. Load order items", (x + 8 * COL, y + ROW), "order_items",
                       "UPSERT", "order_id,line_no")
    put_alerts = put_db("12. Load alerts", (x + 8 * COL, y - 2 * ROW), "alerts")
    put_quarantine = put_db("15. Load quarantine", (x + 7 * COL, y + 3 * ROW),
                            "quarantine_records")

    # ---- reject convergence -------------------------------------------------
    def reason(name, pos, value):
        return proc("UpdateAttribute", name, pos, {"quarantine_reason": value})

    # SCHEMA_INVALID is retired. The SSIS side reports at a finer grain and
    # reject reasons are compared literally, so one bucket could not be matched
    # against its two.
    r_null = reason("R1. reason NULL_CRITICAL", (x + 3 * COL - 160, y + 2 * ROW),
                    "NULL_CRITICAL")
    r_parse = reason("R1b. reason PARSE_ERROR", (x + 4 * COL, y + 2 * ROW),
                     "PARSE_ERROR")
    r_sku = reason("R2. reason UNKNOWN_SKU", (x + 5 * COL, y + 2 * ROW), "UNKNOWN_SKU")
    r_cust = reason("R3. reason UNKNOWN_CUSTOMER", (x + 6 * COL, y + 2 * ROW),
                    "UNKNOWN_CUSTOMER")
    # One label per business rule. These names match the SSIS side's vocabulary
    # so the two engines' reject tables can be compared row for row.
    r_range = reason("R4. reason RANGE_VIOLATION", (x + 7 * COL, y + 2 * ROW),
                     "RANGE_VIOLATION")
    r_currency = reason("R5. reason BAD_CURRENCY", (x + 8 * COL, y + 2 * ROW),
                        "BAD_CURRENCY")
    r_timestamp = reason("R6. reason BAD_TIMESTAMP", (x + 9 * COL, y + 2 * ROW),
                         "BAD_TIMESTAMP")
    r_dup = reason("R7. reason DUP_KEY", (x + 10 * COL, y + 2 * ROW), "DUP_KEY")

    quarantine_funnel = nifi.create_funnel(gid, (x + 6 * COL + 200, y + 3 * ROW))["id"]

    # UpdateRecord, not QueryRecord: rejected records are missing fields by
    # definition, so SQL over them would fail on unknown columns. RecordPath
    # assignment just adds the field.
    shape_rejects = proc("UpdateRecord", "14. Stamp reject reason",
                         (x + 6 * COL + 260, y + 3 * ROW), {
        "record-reader": reader,
        "record-writer": quarantine_writer,
        "replacement-value-strategy": "literal-value",
        "/reason": "${quarantine_reason}",
        # ValidateRecord writes its explanation into this attribute; carrying it
        # into the table is what turns "23 rejected" into "why they were rejected"
        "/detail": "${validation.details}",
        "/batch_id": "${batch_id}",
        "/source_file": "${source_file}",
    })

    alert_funnel = nifi.create_funnel(gid, (x + 8 * COL - 120, y - 2 * ROW))["id"]

    # ---- job run bookkeeping ------------------------------------------------
    job_ok = proc("PutSQL", "13. Record job run", (x + 9 * COL, y - ROW), {
        "JDBC Connection Pool": pool,
        "putsql-sql-statement": (
            # record.count here is the AGGREGATED order count, not lines
            "INSERT INTO job_runs (batch_id, source_file, started_at, finished_at,"
            " duration_ms, orders_loaded, status) VALUES ("
            "'${batch_id}', '${source_file}',"
            " to_timestamp(${ingest_start}/1000.0), now(),"
            " ${now():toNumber():minus(${ingest_start})},"
            " ${record.count}, 'LOADED')"
            " ON CONFLICT (batch_id) DO UPDATE SET"
            " finished_at = now(),"
            " started_at = EXCLUDED.started_at,"
            " duration_ms = EXCLUDED.duration_ms,"
            " orders_loaded = EXCLUDED.orders_loaded,"
            # never downgrade a job that already failed
            " status = CASE WHEN job_runs.status = 'FAILED' THEN 'FAILED'"
            "               ELSE 'LOADED' END"
        ),
    }, retry_rels=["failure"])

    job_rejects = proc("PutSQL", "16. Record rejects", (x + 8 * COL, y + 3 * ROW), {
        "JDBC Connection Pool": pool,
        "putsql-sql-statement": (
            "INSERT INTO job_runs (batch_id, source_file, finished_at,"
            " records_invalid, status) VALUES ("
            "'${batch_id}', '${source_file}', now(), ${record.count}, 'LOADED')"
            " ON CONFLICT (batch_id) DO UPDATE SET"
            " records_invalid = job_runs.records_invalid + EXCLUDED.records_invalid"
        ),
    }, retry_rels=["failure"])

    # records_valid must be counted on the LINE branch: the orders branch has
    # already collapsed lines into orders, so counting there would compare
    # rejected lines against loaded orders and overstate the reject rate.
    job_lines = proc("PutSQL", "19. Record loaded lines", (x + 9 * COL, y + ROW), {
        "JDBC Connection Pool": pool,
        "putsql-sql-statement": (
            "INSERT INTO job_runs (batch_id, source_file, finished_at,"
            " records_valid, status) VALUES ("
            "'${batch_id}', '${source_file}', now(), ${record.count}, 'LOADED')"
            " ON CONFLICT (batch_id) DO UPDATE SET"
            " records_valid = EXCLUDED.records_valid"
        ),
    }, auto_term=["success"], retry_rels=["failure"])

    # ---- failure convergence ------------------------------------------------
    dlq_funnel = nifi.create_funnel(gid, (x + 4 * COL, y + 4 * ROW))["id"]
    dlq_write = proc("PutFile", "17. Park failures in DLQ", (x + 5 * COL, y + 4 * ROW), {
        "Directory": f"{DATA_DIR}/dlq",
        "Conflict Resolution Strategy": "replace",
        "Create Missing Directories": "true",
    }, auto_term=["failure"])

    # Without this, a file that fails to parse would leave NO job_runs row at
    # all -- the one case an ETL job monitor most needs to show. FetchFile can
    # fail before the batch id is stamped, so fall back to the filename.
    batch_or_file = ("${batch_id:isEmpty():ifElse("
                     "${filename:substringBeforeLast('.')},${batch_id})}")
    job_failed = proc("PutSQL", "18. Record failed job", (x + 6 * COL, y + 4 * ROW), {
        "JDBC Connection Pool": pool,
        "putsql-sql-statement": (
            "INSERT INTO job_runs (batch_id, source_file, finished_at, status,"
            " error_text) VALUES ("
            f"'{batch_or_file}', '${{filename}}', now(), 'FAILED',"
            " 'routed to DLQ: ${validation.details:isEmpty()"
            ":ifElse('reader or load failure',${validation.details})}')"
            " ON CONFLICT (batch_id) DO UPDATE SET"
            " status = 'FAILED',"
            " finished_at = now(),"
            " error_text = EXCLUDED.error_text"
        ),
    }, auto_term=["success"])

    print("[build] processors created")

    # ------------------------------------------------------------------
    # Connections
    # ------------------------------------------------------------------
    def node(pid, kind="PROCESSOR"):
        return {"id": pid, "groupId": gid, "type": kind}

    def link(src, dst, rels, name="", src_kind="PROCESSOR", dst_kind="PROCESSOR",
             backpressure=None):
        nifi.connect(gid, node(src, src_kind), node(dst, dst_kind), rels, name,
                     backpressure)

    link(list_file, fetch_file, ["success"])
    link(fetch_file, stamp, ["success"])
    link(stamp, empty_guard, ["success"])
    link(empty_guard, require_fields, ["unmatched"])

    # Required fields first, exactly as the SSIS ladder does it: what reaches
    # ValidateRecord has every critical field present, so an "invalid" there
    # can only mean the value could not be parsed.
    link(require_fields, validate, ["present"])
    link(require_fields, r_null, ["null_critical"])

    link(validate, lookup_sku, ["valid"])
    link(validate, r_parse, ["invalid"])
    link(lookup_sku, lookup_cust, ["matched"])
    link(lookup_sku, r_sku, ["unmatched"])
    link(lookup_cust, rules, ["matched"])
    link(lookup_cust, r_cust, ["unmatched"])

    link(rules, dedupe, ["clean"])
    link(dedupe, replay_check, ["unique"])
    link(dedupe, r_dup, ["dup_key"])
    link(replay_check, orders_q, ["unmatched"])
    link(replay_check, items_q, ["unmatched"])
    link(replay_check, r_dup, ["matched"])
    link(rules, r_range, ["range_violation"])
    link(rules, r_currency, ["bad_currency"])
    link(rules, r_timestamp, ["bad_timestamp"])

    link(orders_q, put_orders, ["orders"])
    link(orders_q, alert_funnel, ["high_value", "suspicious"], dst_kind="FUNNEL")
    link(alert_funnel, put_alerts, ["", ], src_kind="FUNNEL")
    link(items_q, put_items, ["items"])
    link(put_items, job_lines, ["success"])
    link(put_orders, job_ok, ["success"])

    for reason_proc in (r_null, r_parse, r_sku, r_cust,
                        r_range, r_currency, r_timestamp, r_dup):
        link(reason_proc, quarantine_funnel, ["success"], dst_kind="FUNNEL")
    link(quarantine_funnel, shape_rejects, [""], src_kind="FUNNEL")
    link(shape_rejects, put_quarantine, ["success"])
    link(put_quarantine, job_rejects, ["success"])

    # failures all converge on the DLQ
    for src, rels in [
        (fetch_file, ["failure"]),
        (require_fields, ["failure"]),
        (dedupe, ["failure"]),
        (replay_check, ["failure"]),
        (validate, ["failure"]),
        (lookup_sku, ["failure"]),
        (lookup_cust, ["failure"]),
        (rules, ["failure"]),
        (orders_q, ["failure"]),
        (items_q, ["failure"]),
        (shape_rejects, ["failure"]),
        (put_orders, ["failure"]),
        (put_items, ["failure"]),
        (job_lines, ["failure"]),
        (put_alerts, ["failure"]),
        (put_quarantine, ["failure"]),
        (job_ok, ["failure"]),
        (job_rejects, ["failure"]),
    ]:
        link(src, dlq_funnel, rels, dst_kind="FUNNEL")
    link(empty_guard, dlq_funnel, ["empty"], dst_kind="FUNNEL")
    link(dlq_funnel, dlq_write, [""], src_kind="FUNNEL")
    link(dlq_write, job_failed, ["success"])
    link(job_failed, dlq_funnel, ["failure"], dst_kind="FUNNEL")

    # retry loops back on itself -- a transient DB blip should not lose data
    for pid in (put_orders, put_items, put_alerts, put_quarantine, job_ok,
                job_rejects, job_failed, job_lines):
        link(pid, pid, ["retry"])

    # terminal sinks
    nifi.update_processor(put_alerts, {"config": {
        "autoTerminatedRelationships": ["success"]}})
    for pid in (job_ok, job_rejects):
        nifi.update_processor(pid, {"config": {
            "autoTerminatedRelationships": ["success"]}})

    print("[build] connections created")

    nifi.enable_all_services(gid)
    nifi.wait_for_services_enabled(gid)
    print(f"[build] done. Process group id: {gid}")
    return 0
