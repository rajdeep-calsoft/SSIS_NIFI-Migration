import json
import os

DS = {"type": "grafana-postgresql-datasource", "uid": "etl-postgres"}
# NOTE: there is also an `ssis-postgres` datasource provisioned, pointing at
# the other engine's warehouse. No panel here uses it any more -- the
# comparison board reads that warehouse through postgres_fdw instead, so its
# queries can JOIN the two engines rather than run beside each other. The
# datasource is kept for ad-hoc exploration in Grafana's Explore tab.

# Validated categorical slots (dataviz reference palette, all-pairs safe trio).
# These are the palette's DARK steps, not its light ones, because Grafana
# paints on a dark surface (#1a1a19) by default. The light steps fail the dark
# lightness band -- #eb6834 sits at L 0.671 against a 0.48-0.67 band -- so the
# board was being drawn with colours validated for a surface it never uses.
# Checked with the skill's validator, all three pairs, on the dark surface:
#   lightness PASS · chroma PASS · CVD dE 9.4 worst (deutan) · normal dE 20.9
#   · contrast PASS
#
# BLUE IS ALWAYS NiFi AND ORANGE IS ALWAYS SSIS, on every board and in every
# panel. Colour follows the engine, never the rank or the column position.
BLUE, ORANGE, AQUA = "#3987e5", "#d95926", "#199e70"
# Status palette - fixed, never themed, never reused as a series colour. These
# are the same four steps in both modes and all clear 3:1 on the dark surface.
GOOD, WARN, SERIOUS, CRITICAL = "#0ca30c", "#fab219", "#ec835a", "#d03b3b"

# ---------------------------------------------------------------------------
# SEMANTIC ROLES. Pick a colour by what it MEANS, never by which panel it is.
#
# The rule that was being broken: colour must follow the entity. The SSIS
# health board painted its own throughput BLUE -- the colour that means NiFi on
# the comparison board -- while the NiFi board painted its rejects ORANGE, the
# colour that means SSIS. A reviewer flipping between :3000 and :3001 was being
# shown the other engine's colour on each. So:
#
#   * a categorical hue identifies an ENGINE, and only an engine. Each
#     single-engine board paints its own throughput in its own engine's hue.
#   * anything that is a STATE rather than an entity -- rejects, reject rate,
#     queue pressure -- wears the status palette, which is reserved and never
#     impersonates a series.
#   * AQUA is the third entity, used for things that belong to neither engine
#     (money, alert types, file counts).
#
# Checked on the dark surface with the skill's validator: engine pair
# CVD dE 24.7, and SSIS orange vs warning amber dE 19.1 -- clear of the
# 8 target, so the two never read as each other.
ENGINE_NIFI = BLUE        # identity: this engine, on every board
ENGINE_SSIS = ORANGE      # identity: that engine, on every board
QUALITY = WARN            # rejects, reject rate, reject reasons - a state
PRESSURE = SERIOUS        # queue depth / backlog - a state
NEUTRAL = AQUA            # belongs to neither engine (money, alerts, files)


def target(sql, fmt="time_series", ds=None):
    return [{"refId": "A", "format": fmt, "rawQuery": True, "rawSql": sql,
             "datasource": ds or DS, "editorMode": "code"}]


def stat(title, sql, x, y, w=5, h=4, unit="short", color=BLUE, steps=None,
         desc="", ds=None):
    thresholds = {"mode": "absolute",
                  "steps": steps or [{"color": color, "value": None}]}
    return {
        "type": "stat", "title": title, "description": desc,
        "datasource": ds or DS, "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": target(sql, "table", ds),
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": thresholds,
                                     "mappings": []}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "",
                                      "values": False},
                    "colorMode": "value", "graphMode": "none",
                    "textMode": "auto", "justifyMode": "auto"},
    }


def timeseries(title, sql, x, y, w=12, h=8, colors=None, unit="short",
               fill=8, desc="", stack=False, ds=None):
    overrides = []
    for name, hexv in (colors or {}).items():
        overrides.append({
            "matcher": {"id": "byName", "options": name},
            "properties": [{"id": "color",
                            "value": {"mode": "fixed", "fixedColor": hexv}}],
        })
    return {
        "type": "timeseries", "title": title, "description": desc,
        "datasource": ds or DS, "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": target(sql, ds=ds),
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    # thin marks, soft fill, no point clutter until you hover
                    "lineWidth": 2, "fillOpacity": fill, "showPoints": "never",
                    "lineInterpolation": "smooth", "spanNulls": True,
                    "gradientMode": "opacity",
                    "stacking": {"mode": "normal" if stack else "none",
                                 "group": "A"},
                    "axisSoftMin": 0,
                },
                "thresholds": {"mode": "absolute",
                               "steps": [{"color": "text", "value": None}]},
            },
            "overrides": overrides,
        },
        "options": {
            # legend always present for >= 2 series; crosshair tooltip by default
            "legend": {"displayMode": "list", "placement": "bottom",
                       "showLegend": True, "calcs": []},
            "tooltip": {"mode": "multi", "sort": "none"},
        },
    }


def barchart(title, sql, x, y, w=12, h=8, color=BLUE, unit="short", desc="",
             ds=None):
    return {
        "type": "barchart", "title": title, "description": desc,
        "datasource": ds or DS, "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": target(sql, "table", ds),
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "fixed", "fixedColor": color},
                "custom": {
                    "lineWidth": 0,
                    "fillOpacity": 85,
                    "gradientMode": "none",
                    "axisSoftMin": 0,
                    # Every bar carries its own number (showValue below), so
                    # the vertical gridlines were measuring something already
                    # written on the mark. Recessive axes, and one less thing
                    # crossing the fills.
                    "axisGridShow": False,
                    "axisBorderShow": False,
                },
            },
            "overrides": [],
        },
        "options": {
            "orientation": "horizontal",
            # SPACE BETWEEN THE BARS -- and these two live HERE, in `options`,
            # not in fieldConfig.defaults.custom where they were.
            #
            # Grafana silently drops unknown keys from `custom`, so the old
            # barWidth/barRadius were never read and the panel had been drawing
            # at the default 0.97 all along: eight reject reasons, all within
            # 20% of each other, fused into one amber slab you could not count
            # the bars in. Nothing errored, and nothing said so.
            #
            # barWidth is the fraction of each category slot the bar fills, so
            # the gap is the remainder. 0.58 keeps the mark substantial enough to carry
            # its label while leaving a clear band of surface between rows.
            "barWidth": 0.58,
            # Rounded data-end. The bar stays square where it starts and is
            # rounded only where it stops, so the end you read the value from
            # is the end that is shaped.
            "barRadius": 0.25,
            # direct value labels: the relief rule for sub-3:1 fills
            "showValue": "always",
            "text": {"valueSize": 12},
            "legend": {"showLegend": False},
            "tooltip": {"mode": "single"},
            "xTickLabelRotation": 0,
        },
    }


def width_overrides(widths):
    """Explicit column widths. Grafana's auto-fit truncates long ids to
    unreadable stubs -- two different columns can end up rendering the same
    visible text, which is worse than useless in a table meant to identify
    rows."""
    out = []
    for name, px in (widths or {}).items():
        out.append({"matcher": {"id": "byName", "options": name},
                    "properties": [{"id": "custom.width", "value": px}]})
    return out


def unit_override(name, unit, decimals=None):
    props = [{"id": "unit", "value": unit}]
    if decimals is not None:
        props.append({"id": "decimals", "value": decimals})
    return {"matcher": {"id": "byName", "options": name}, "properties": props}


def table(title, sql, x, y, w=24, h=9, overrides=None, desc="", widths=None,
          ds=None, cell_height="sm"):
    return {
        "type": "table", "title": title, "description": desc,
        "datasource": ds or DS, "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": target(sql, "table", ds),
        "fieldConfig": {
            "defaults": {"custom": {"align": "auto", "filterable": False,
                                    "cellOptions": {"type": "auto"}},
                         "mappings": []},
            "overrides": (overrides or []) + width_overrides(widths),
        },
        "options": {"showHeader": True, "footer": {"show": False},
                    "cellHeight": cell_height},
    }


STATUS_OVERRIDE = [{
    "matcher": {"id": "byName", "options": "status"},
    "properties": [
        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
        # BOTH engines' status vocabularies, mapped to the same four colours.
        # The words genuinely differ -- his runner writes PASS/FAIL, this one
        # derives RUNNING/SUCCESS/PARTIAL/FAILED -- and changing either would
        # be a behaviour change, not a design one. So the COLOUR is what is
        # made identical: green is good and red is bad on both boards, and his
        # status column stopped rendering as undifferentiated white text.
        {"id": "mappings", "value": [{"type": "value", "options": {
            "RUNNING": {"color": BLUE, "index": 0, "text": "RUNNING"},
            "SUCCESS": {"color": GOOD, "index": 1, "text": "SUCCESS"},
            "PASS": {"color": GOOD, "index": 2, "text": "PASS"},
            "PARTIAL": {"color": WARN, "index": 3, "text": "PARTIAL"},
            "FAILED": {"color": CRITICAL, "index": 4, "text": "FAILED"},
            "FAIL": {"color": CRITICAL, "index": 5, "text": "FAIL"},
        }}]},
    ],
}]

SEVERITY_OVERRIDE = [{
    "matcher": {"id": "byName", "options": "severity"},
    "properties": [
        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
        {"id": "mappings", "value": [{"type": "value", "options": {
            "INFO": {"color": BLUE, "index": 0},
            "WARN": {"color": WARN, "index": 1},
            "CRITICAL": {"color": CRITICAL, "index": 2},
        }}]},
    ],
}]

# Every reject reason wears the SAME amber the "Why records were rejected" bar
# chart uses, on both engines. A reason is a state, not an entity, so it takes
# the status palette and never a categorical hue -- and giving each reason its
# own colour would be colour-by-rank, which is the one thing colour must never
# follow. Amber everywhere means "this record was refused", full stop.
REASON_OVERRIDE = [{
    "matcher": {"id": "byName", "options": "reason"},
    "properties": [
        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
        {"id": "color", "value": {"mode": "fixed", "fixedColor": QUALITY}},
    ],
}]

# NEVER to_char() A TIMESTAMP FOR DISPLAY. Send the real column and let Grafana
# render it in the dashboard's timezone ("browser").
#
# to_char() formats in the DATABASE's timezone, which is Etc/UTC in both
# containers. Grafana renders every other timestamp column in the VIEWER's. So
# a board carried both: the file ledger said 18:30 and the reject log beside it
# said 13:00 for the same second, a 5:30 gap on one screen, and the reject log
# looked five hours stale -- exactly like a panel that had stopped being live.
# It was live; only its clock was wrong.
#
# His `detected_at` is `timestamp WITHOUT time zone` holding UTC, so it gets an
# explicit AT TIME ZONE 'UTC' rather than relying on the driver to assume it.

# One column layout for the FILE LEDGER, shared by the pair for the same
# reason: the two tables are meant to be read against each other, so they line
# up pixel for pixel and the eye does not have to re-find a column.
# Size the two TEXT columns, let the six numeric ones share what is left.
#
# Grafana hands leftover grid space to the columns that have no width set, so
# the choice is really "which columns absorb the slack". Sizing everything but
# `revenue` pushed the money to the far edge behind a wide gap; sizing
# everything but `source_file` put the same gap in the middle; sizing NOTHING
# spread all eight evenly and truncated the filename to `…T135407_575d9`,
# which is the one column that identifies the row.
#
# Six short numeric columns sharing the remainder each get a sane width and
# stay readable at any window size, and the filename keeps the room it needs.
LEDGER_WIDTHS = {"processed_at": 185, "source_file": 330}

# `date_trunc('second', ...)` removes the milliseconds from the VALUE, but
# Grafana still renders a timestamp field with its own default precision and
# prints a trailing ".00". An explicit format is the only thing that stops it.
TS_FORMAT = "time:YYYY-MM-DD HH:mm:ss"

LEDGER_OVERRIDES = STATUS_OVERRIDE + [
    unit_override("processed_at", TS_FORMAT),
    unit_override("revenue", "currencyINR", 0),
]

# One column layout for the reject log, used by BOTH engines' boards so the
# two tables line up column-for-column when you flip between :3000 and :3001.
REJECT_WIDTHS = {"at": 185, "order_id": 245, "line_no": 85, "sku": 110,
                 "qty": 70, "unit_price": 105, "currency": 95, "reason": 175}
# `detail` stays unsized here, on purpose -- it is the long one, so it is the
# right column to hand the leftover width to.
REJECT_OVERRIDES = REASON_OVERRIDE + [unit_override("at", TS_FORMAT)]

LEVEL_OVERRIDE = [{
    "matcher": {"id": "byName", "options": "level"},
    "properties": [
        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
        {"id": "mappings", "value": [{"type": "value", "options": {
            "INFO": {"color": BLUE, "index": 0},
            "WARNING": {"color": WARN, "index": 1},
            "ERROR": {"color": CRITICAL, "index": 2},
        }}]},
    ],
}]


def dashboard(uid, title, description, panels, tags):
    return {
        "uid": uid, "title": title, "description": description, "tags": tags,
        "schemaVersion": 39, "version": 1, "editable": True,
        "refresh": "5s", "timezone": "browser",
        "time": {"from": "now-15m", "to": "now"},
        "timepicker": {"refresh_intervals": ["1s", "5s", "10s", "30s", "1m", "5m"]},
        "panels": panels,
    }


# ---------------------------------------------------------------- health ----
# Same rhythm as the comparison board, deliberately: four tiles, two panels,
# one detail table. Twelve panels became seven.
#
# What went, and why:
#   Line items loaded   orders already answers "is it moving"; the exact line
#                       count is in the ledger below, per file.
#   Queue depth (tile)  }  backpressure is worth watching during a `flood`,
#   NiFi queue depth    }  and NiFi's OWN canvas at :8080 shows it far better
#   JVM heap            }  than a Grafana line -- you watch the queues fill on
#                       the diagram itself. Still recorded in nifi_metrics, and
#                       `make watch` prints it live.
#   Reject rate (chart) the number is now a tile; the shape over time was the
#                       same story told twice on one screen.
#
# Kept deliberately: the bulletins table. It is the ONLY place NiFi's own
# internal errors surface -- they are not rejects and appear in no other panel.
health = dashboard(
    "nifi-pipeline-health", "1 - Pipeline Health",
    "Is the NiFi ETL pipeline healthy right now? Four numbers, what it is "
    "doing, why it is refusing anything, and the per-file ledger.",
    [
        # --- row 1: is it healthy? ---------------------------------------
        stat("Orders loaded", "SELECT count(*) AS v FROM orders", 0, 0, w=6,
             color=ENGINE_NIFI, desc="Rows in the orders table."),
        stat("Records rejected",
             "SELECT count(*) AS v FROM quarantine_records", 6, 0, w=6,
             steps=[{"color": GOOD, "value": None}, {"color": WARN, "value": 1}],
             desc="Records the flow refused to load, across all reject "
                  "reasons. A count, not a rate -- it only ever grows."),
        stat("Reject rate",
             "SELECT CASE WHEN coalesce(sum(records_valid + records_invalid),0) = 0"
             " THEN 0 ELSE round(100.0 * sum(records_invalid)"
             " / sum(records_valid + records_invalid), 2) END AS v FROM job_runs",
             12, 0, w=6, unit="percent",
             steps=[{"color": GOOD, "value": None}, {"color": WARN, "value": 25},
                    {"color": CRITICAL, "value": 90}],
             desc="Share of arriving lines refused. THIS is the health number, "
                  "not the count beside it: a steady defect rate shows as a "
                  "growing count but a flat percentage. Above 25% a batch is "
                  "marked PARTIAL; 100% means nothing loaded at all."),
        # "Failed files", not "Failed jobs" -- one job IS one file here, and the
        # SSIS board calls it that. A tile that counts the same thing under a
        # different name is the kind of difference a reader stops to resolve.
        stat("Failed files",
             "SELECT count(*) AS v FROM v_job_runs_recent WHERE status='FAILED'",
             18, 0, w=6,
             steps=[{"color": GOOD, "value": None},
                    {"color": CRITICAL, "value": 1}],
             desc="Files this engine could not process cleanly."),

        # --- row 2: what is it doing, and why is it refusing anything? ----
        # LINES, not orders -- his board plots lines loaded, and two charts
        # sharing a title, a position and an axis while counting different
        # things is worse than two different-looking boards.
        timeseries("Ingest throughput", (
            "SELECT $__timeGroupAlias(ingest_ts, $__interval),"
            " count(*) AS \"lines loaded\" FROM order_items"
            " WHERE $__timeFilter(ingest_ts) GROUP BY 1 ORDER BY 1"),
            0, 4, colors={"lines loaded": ENGINE_NIFI},
            desc="Lines landing in Postgres, by the time NiFi loaded them. A "
                 "flat line here while rejects climb means the flow is stalled "
                 "-- run `make smoke`, it will name the stopped processor."),
        barchart("Why records were rejected", (
            "SELECT reason, count(*) AS records FROM quarantine_records"
            " GROUP BY reason ORDER BY records DESC"),
            12, 4, color=QUALITY,
            desc="Each reason maps to a different processor in the flow. The "
                 "SSIS board draws the same chart from its own warehouse, so "
                 "the two can be read against each other."),

        # --- row 3: the detail ---------------------------------------------
        # The SSIS board's ledger, column for column and name for name. This
        # panel used to show batch_id / duration_ms / reject_pct / error_text
        # -- NiFi's own vocabulary -- against his processed_at / source_file /
        # lines_in / lines_clean / lines_rejected / revenue. Same position,
        # same purpose, eight different headers: the single biggest reason the
        # two boards did not read as one picture.
        #
        # Nothing is computed differently, only named and selected his way:
        # lines_in is this engine's records_valid + records_invalid, which is
        # exactly what its reject-rate tile already divides by. revenue is the
        # one column job_runs does not carry, so it is summed from the lines
        # that batch actually loaded.
        table("File ledger - one row per input file", (
            "SELECT j.finished_at AS processed_at, j.source_file, j.status,"
            " j.records_valid + j.records_invalid AS lines_in,"
            " j.records_valid   AS lines_clean,"
            " j.records_invalid AS lines_rejected,"
            " j.orders_loaded, coalesce(r.revenue, 0) AS revenue"
            " FROM v_job_runs_recent j"
            " LEFT JOIN (SELECT batch_id, sum(line_total) AS revenue"
            "              FROM order_items GROUP BY batch_id) r"
            "        ON r.batch_id = j.batch_id"
            " ORDER BY j.finished_at DESC LIMIT 25"),
            0, 12, overrides=LEDGER_OVERRIDES, widths=LEDGER_WIDTHS,
            desc="This engine's job ledger. Every file that arrived leaves a "
                 "row. The SSIS board draws the same eight columns from "
                 "control.stream_file, so the two line up row against row."),

        # --- row 4: the rejection log --------------------------------------
        # Its twin sits at this exact position on the SSIS board, with the
        # same nine columns in the same order, so a reader flipping between
        # :3000 and :3001 compares rows without re-reading the header.
        table("Rejection log - every record this engine refused", (
            "SELECT date_trunc('second', quarantined_at) AS at,"
            " order_id, line_no, sku, qty, unit_price, currency,"
            " reason, detail FROM quarantine_records"
            " ORDER BY quarantined_at DESC, id DESC LIMIT 25"),
            0, 21, overrides=REJECT_OVERRIDES, widths=REJECT_WIDTHS,
            desc="The record, the reason, and the offending value -- newest "
                 "first. The bar chart above counts these; this is the rows "
                 "themselves. Same nine columns as the SSIS board.\n\n"
                 "All of it, in a shell:\n"
                 "  make errors           both engines, with a reason diff\n"
                 "  make errors-export    the same as CSV + JSON\n"
                 "  docker compose exec postgres psql -U etl -d etldemo\n"
                 "      SELECT * FROM quarantine_records ORDER BY id DESC;\n\n"
                 "For NiFi's OWN errors -- which are not rejects; a rejected "
                 "record is the pipeline working correctly -- use `make logs "
                 "S=nifi`, the canvas at :8080, or the nifi_bulletins table."),
    ],
    ["nifi", "etl", "monitoring"])

# -------------------------------------------------------------- business ----
# Adapted to the SSIS engine's own live board, and cut to the same 4-2-1
# skeleton every other board on this project uses: four tiles, two charts, one
# full-width table.
#
# Why these four measures and not the ones that were here: this board and the
# SSIS one below are now a matched PAIR, the way the two health boards are, so
# a measure only earns a tile if BOTH warehouses can answer it the same way.
#
#   Orders / Lines / Revenue / Alerts   both engines, identical arithmetic.
#                                       Revenue is now the sum of LINE totals,
#                                       matching his sum(total_price), rather
#                                       than orders.order_total -- the same
#                                       money, but summed the way he sums it.
#
# What went, and why:
#   Avg order value      derivable from the two tiles beside it.
#   Revenue by category  } his warehouse keys the fact table on product_key
#   Top products         } and joins the dimension differently, so neither
#                        panel had an honest twin. They were the two panels
#                        that made this board un-mirrorable.
DRILL = ("\n\nIn a shell: `make watch` for live row counts, or\n"
         "  docker compose exec postgres psql -U etl -d etldemo")
DRILL_HIS = ("\n\nIn a shell: `docker logs -f ssis-engine`, or\n"
             "  docker exec -it ssis-warehouse psql -U etl_user -d etl_db")


def live_board(uid, title, engine, ds, sql, drill):
    """Both live boards, one definition.

    The pair exists to be flipped between, so they cannot be allowed to drift
    apart panel by panel the way two hand-maintained boards do. Only the SQL
    and the engine colour are arguments; the shape is not.
    """
    def panel(fn, *a, **kw):
        kw["ds"] = ds
        return fn(*a, **kw)

    return dashboard(
        uid, title,
        "What the pipeline is actually delivering: live orders, lines, "
        "revenue, and the in-stream alert rules firing.",
        [
            # --- row 1: what has it delivered? ---------------------------
            panel(stat, "Orders", sql["orders"], 0, 0, w=6, color=engine,
                  desc="Distinct orders in the warehouse." + drill),
            # NEUTRAL, not the engine hue, because the `lines` SERIES in the
            # chart below is neutral -- a tile and a series that are the same
            # measure must be the same colour, or the reader has to learn the
            # palette twice on one screen.
            panel(stat, "Lines", sql["lines"], 6, 0, w=6, color=NEUTRAL,
                  desc="Order lines loaded. Always >= orders; the ratio is "
                       "the average basket size." + drill),
            panel(stat, "Revenue", sql["revenue"], 12, 0, w=6,
                  unit="currencyINR", color=NEUTRAL,
                  desc="Sum of every loaded line total. Money belongs to "
                       "neither engine, so it is never painted in an engine's "
                       "colour." + drill),
            panel(stat, "Alerts raised", sql["alerts"], 18, 0, w=6,
                  steps=[{"color": GOOD, "value": None},
                         {"color": WARN, "value": 1}],
                  desc="In-stream rule hits: HIGH_VALUE and SUSPICIOUS_QTY. "
                       "An alert is not a reject -- the record still "
                       "loaded." + drill),

            # --- row 2: what is it doing right now? ----------------------
            # Two series on ONE axis, both counts, so they are directly
            # comparable -- never a second y-scale. Orders wears the engine's
            # own hue; lines wears the neutral slot, the same way revenue
            # does, because it is a volume measure rather than an identity.
            panel(timeseries, "Orders and lines per minute", sql["rate"],
                  0, 4, colors={"orders": engine, "lines": NEUTRAL},
                  desc="Both series count the same stream at two grains. A "
                       "flat line here is a stalled pipeline, whatever the "
                       "tiles above say -- they are running totals and never "
                       "go down."),
            panel(timeseries, "Revenue per minute", sql["revenue_rate"],
                  12, 4, colors={"revenue": NEUTRAL}, unit="currencyINR"),

            # --- row 3: the detail ---------------------------------------
            # Six columns, same order on both boards. Severity and the
            # customer name are gone from the NiFi side: his alerts table has
            # neither, and a column that exists on only one of a matched pair
            # is the thing that makes two boards stop being comparable.
            panel(table, "Live alerts - the in-stream rules firing",
                  sql["alerts_log"], 0, 12,
                  overrides=[unit_override("value", "currencyINR", 0),
                             unit_override("at", TS_FORMAT)],
                  widths={"at": 185, "alert_type": 165, "order_id": 245,
                          "customer_id": 130, "value": 120},
                  desc="Written by the engine's own rules as the data streams "
                       "past. The rejection log lives on the Pipeline Health "
                       "board, not here: a rejected record never arrives, an "
                       "alerted one did." + drill),
        ],
        ["live", "business"])


business = live_board(
    "ecommerce-live", "2 - Business Live", ENGINE_NIFI, DS, {
        "orders": "SELECT count(*) AS v FROM orders",
        "lines": "SELECT count(*) AS v FROM order_items",
        "revenue":
            "SELECT round(coalesce(sum(line_total),0)) AS v FROM order_items",
        "alerts": "SELECT count(*) AS v FROM alerts",
        "rate": (
            "SELECT $__timeGroupAlias(ingest_ts, $__interval),"
            " count(DISTINCT order_id) AS \"orders\", count(*) AS \"lines\""
            " FROM order_items WHERE $__timeFilter(ingest_ts)"
            " GROUP BY 1 ORDER BY 1"),
        "revenue_rate": (
            "SELECT $__timeGroupAlias(ingest_ts, $__interval),"
            " sum(line_total) AS \"revenue\" FROM order_items"
            " WHERE $__timeFilter(ingest_ts) GROUP BY 1 ORDER BY 1"),
        "alerts_log": (
            "SELECT date_trunc('second', alert_ts) AS at, alert_type,"
            " order_id, customer_id, round(metric_value) AS value, detail"
            " FROM alerts ORDER BY alert_ts DESC, id DESC LIMIT 25"),
    }, DRILL)

# ===========================================================================
# Dashboard 3: the two engines, side by side.
#
# The question this answers is not "how is the pipeline doing" but "do the two
# engines agree". Colour carries the ENGINE and nothing else: NiFi is always
# blue, SSIS is always orange, in every panel. A reader who learns the pair
# once can read the whole board. (Both are slots from the validated
# categorical trio at the top of this file, so the pairing is colourblind-safe.)
#
# This board used to run two independent queries per panel against two
# datasources and join the frames in Grafana. That is gone -- see below.
# ===========================================================================

# Every panel below reads ONE datasource -- this warehouse -- because
# db/init/04_ssis_fdw.sql makes the SSIS warehouse readable from it. That
# single change is what lets the board stop showing two independent numbers
# and start showing a JOIN:
#
#   * totals are restricted to files BOTH engines have finished, so a file in
#     flight never looks like a disagreement (it used to: "6.10 K vs 6.19 K"
#     was one file, not a bug);
#   * the reject sets are FULL OUTER JOINed, so a record one engine rejected
#     and the other loaded is a ROW, not a subtraction the reader performs;
#   * the reject log covers both engines instead of only NiFi's.
#
# It is also much faster. The old mixed-datasource panels took ~40s to paint.

# No `pair_stat` here any more. Eight of this board's sixteen panels used to be
# stat tiles in NiFi/SSIS pairs -- "Orders · NiFi" beside "Orders · SSIS" and so
# on -- which made the reader compare two big numbers by eye, four times over,
# and said nothing about whether they matched. One four-row table does the same
# job in a quarter of the space AND answers the question, because it can carry
# a delta column. Comparison is a table's job, not a stat tile's.


# Matched on the DISPLAY name: the queries alias these columns "NiFi" and
# "SSIS" so the headers read as product names rather than sql identifiers.
ENGINE_COLOURS = [
    {"matcher": {"id": "byName", "options": "NiFi"},
     "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                    {"id": "color", "value": {"mode": "fixed", "fixedColor": ENGINE_NIFI}}]},
    {"matcher": {"id": "byName", "options": "SSIS"},
     "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                    {"id": "color", "value": {"mode": "fixed", "fixedColor": ENGINE_SSIS}}]},
]

# Delta is coloured TEXT, not a filled cell. It was a colour-background block
# and the board ended up with three saturated green slabs -- two Delta columns
# and the `agree` column -- all shouting about the case that needs no
# attention. Green should be quiet and red should shout, so exactly ONE column
# on the board carries a filled background (`agree`, a genuine status chip),
# and everything else states its verdict in colour on the panel's own ground.
DELTA_OVERRIDE = [{
    "matcher": {"id": "byName", "options": "Delta"},
    "properties": [
        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
        {"id": "thresholds", "value": {"mode": "absolute", "steps": [
            {"color": CRITICAL, "value": None},
            {"color": GOOD, "value": 0},
            {"color": CRITICAL, "value": 1}]}},
        {"id": "color", "value": {"mode": "thresholds"}},
    ],
}]

# The board answers exactly one question -- do the two engines agree? -- so it
# is built as one answer, one breakdown, one piece of evidence:
#
#   row 1  the verdict           four stat tiles, read left to right
#   row 2  where it comes from   two side-by-side tables, same shape
#   row 3  the evidence          every rejected record, both engines
#
# It had sixteen panels and now has seven. What went, and why:
#
#   8 paired stat tiles   "Orders · NiFi" next to "Orders · SSIS" asks the
#                         reader to compare two big numbers by eye and never
#                         says whether they match. Now four rows of one table
#                         with a delta column that answers it.
#   Per-file verdict      a 20-row table that repeated what "In flight" and
#                         the disagreement rows already say.
#   disagreement table    folded into the reject log: that table already has
#                         an `agree` column, so sorting disagreements to the
#                         top makes one table do both jobs.
#
# Every panel is restricted to files BOTH engines have finished (v_settled_
# files). Without that a file in flight shows up as a difference that then
# resolves itself, and a reader cannot tell those from a real one.
comparison = dashboard(
    "engine-comparison", "NiFi vs SSIS - same data, same answer?",
    "One generator feeds both engines the same batches (make live-compare). "
    "Blue is always NiFi, orange is always SSIS. Read it top to bottom: the "
    "verdict, then where it comes from, then the records themselves. Counts "
    "only files BOTH engines have finished, so a file in flight is never "
    "mistaken for a disagreement.",
    [
        # --- row 1: the verdict ------------------------------------------
        # Agreement is the hero number and gets the widest tile, so it renders
        # largest -- Grafana sizes stat text to the panel.
        stat("Do they agree?",
             "SELECT agreement_pct AS v FROM v_agreement", 0, 0, w=9, h=4,
             unit="percent",
             steps=[{"color": CRITICAL, "value": None},
                    {"color": WARN, "value": 95},
                    {"color": GOOD, "value": 100}],
             desc="Share of settled reject decisions both engines made "
                  "identically. 100% means every record got the same verdict "
                  "AND the same reason from both."),
        stat("Records they differ on",
             "SELECT disagreements AS v FROM v_agreement", 9, 0, w=5, h=4,
             steps=[{"color": GOOD, "value": None},
                    {"color": CRITICAL, "value": 1}],
             desc="A count of records, not a rate. Every one of them is a row "
                  "in the table at the bottom, sorted to the top."),
        stat("Files compared",
             "SELECT files_settled AS v FROM v_compare_totals", 14, 0, w=5,
             h=4, color=NEUTRAL,
             desc="Files BOTH engines have finished. Everything on this board "
                  "is restricted to these."),
        stat("Still in flight",
             "SELECT count(*) AS v FROM v_compare_files"
             " WHERE verdict LIKE 'not in %'", 19, 0, w=5, h=4,
             steps=[{"color": GOOD, "value": None}, {"color": WARN, "value": 3}],
             desc="Files one engine has finished and the other has not. Normal "
                  "and self-correcting -- NiFi picks a file up in about a "
                  "second, the SSIS runner polls every five. Check this first "
                  "when a number looks wrong."),

        # --- row 2: where the verdict comes from --------------------------
        # Both tables are deliberately the SAME four columns in the same
        # order -- label, NiFi, SSIS, delta -- so the eye learns the shape
        # once and reads the second table for free.
        table("What each engine produced", (
            'SELECT m.label AS "Measure", m.nifi AS "NiFi", m.ssis AS "SSIS",'
            '       m.nifi - m.ssis AS "Delta"'
            "  FROM v_compare_totals t,"
            "  LATERAL (VALUES (1,'Orders',      t.nifi_orders,  t.ssis_orders),"
            "                  (2,'Order lines', t.nifi_lines,   t.ssis_lines),"
            "                  (3,'Rejected',    t.nifi_rejects, t.ssis_rejects),"
            "                  (4,'Alerts',      t.nifi_alerts,  t.ssis_alerts))"
            "       AS m(ord,label,nifi,ssis)"
            " ORDER BY m.ord"),
            0, 4, w=12, h=8, cell_height="lg",
            overrides=ENGINE_COLOURS + DELTA_OVERRIDE,
            # Only the VALUE columns get a width. Grafana hands leftover space
            # to the columns that have none, so the label column absorbs it --
            # otherwise the slack lands on `delta`, and a colour-background
            # cell stretched across half the panel reads as a giant green
            # slab rather than a verdict.
            widths={"NiFi": 120, "SSIS": 120, "Delta": 110},
            desc="The four totals that have to match. Order lines matter most: "
                 "if the engines load different rows, nothing below is "
                 "comparable."),

        table("Why records were rejected", (
            'SELECT reason AS "Reason", nifi AS "NiFi", ssis AS "SSIS",'
            '       delta AS "Delta"'
            "  FROM v_compare_reasons ORDER BY abs(delta) DESC, reason"),
            12, 4, w=12, h=8,
            overrides=ENGINE_COLOURS + DELTA_OVERRIDE,
            widths={"NiFi": 120, "SSIS": 120, "Delta": 110},
            desc="A FULL OUTER JOIN, so a reason only one engine produces "
                 "still appears, with a zero on the other side -- the case an "
                 "inner join would hide, and the most serious one, because it "
                 "means an engine is missing a rule. Sorted by the size of the "
                 "difference, so anything non-zero is the first row."),

        # --- row 3: the evidence ------------------------------------------
        table("Every rejected record, both engines", (
            "SELECT date_trunc('second', at) AS at, order_id, line_no,"
            ' nifi_says AS "NiFi says", ssis_says AS "SSIS says", agree,'
            # NiFi's ValidateRecord message is 100 characters of boilerplate
            # with the one useful fact -- WHICH field was the wrong type --
            # last, so the column rendered as the same truncated sentence on
            # every row. Keep the field list, drop the preamble.
            " CASE WHEN why LIKE '%did not match the schema%'"
            "      THEN 'wrong type: ' || replace(replace("
            "           regexp_replace(why, '^.*schema: ', ''), '[',''), ']','')"
            "      ELSE left(coalesce(why, ''), 70) END AS detail"
            " FROM v_reject_side_by_side"
            " ORDER BY (agree = 'same'), at DESC, order_id LIMIT 60"),
            0, 12, w=24, h=12,
            overrides=[
                # The two verdict columns keep the engine colours used
                # everywhere else, so which column is which needs no looking up.
                {"matcher": {"id": "byName", "options": "NiFi says"},
                 "properties": [
                     {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                     {"id": "color", "value": {"mode": "fixed", "fixedColor": ENGINE_NIFI}}]},
                {"matcher": {"id": "byName", "options": "SSIS says"},
                 "properties": [
                     {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                     {"id": "color", "value": {"mode": "fixed", "fixedColor": ENGINE_SSIS}}]},
                # `agree` is the column to read first -- but "first" is not
                # "loudest". When the engines agree (the normal case) this
                # column is every row, so a filled background turned the whole
                # panel into a wall of saturated green advertising the one
                # thing that needs no attention. Coloured text instead: the
                # board stays calm, and because disagreements sort to the top,
                # an amber or red word in row one is impossible to miss.
                {"matcher": {"id": "byName", "options": "agree"},
                 "properties": [
                     {"id": "custom.cellOptions",
                      "value": {"type": "color-text"}},
                     {"id": "mappings", "value": [{"type": "value", "options": {
                         "same":             {"color": GOOD,     "index": 0},
                         "NiFi only":        {"color": WARN,     "index": 1},
                         "SSIS only":        {"color": WARN,     "index": 2},
                         "different reason": {"color": CRITICAL, "index": 3}}}]},
                     # Mappings win over thresholds, so the four known values
                     # get their own colour. The base threshold is a neutral
                     # grey rather than the default green: an UNmapped value
                     # falling through to green would read as "these agree",
                     # which is the one wrong answer this column can give.
                     {"id": "thresholds", "value": {"mode": "absolute",
                      "steps": [{"color": "#5a6570", "value": None}]}},
                     {"id": "color", "value": {"mode": "thresholds"}}]},
                unit_override("at", TS_FORMAT),
            ],
            # `detail` is left unsized on purpose: it is the one column that
            # can usefully absorb the leftover width, which keeps `agree` a
            # narrow status chip instead of a colour-background block half the
            # table wide.
            widths={"at": 185, "order_id": 265, "line_no": 110,
                    "NiFi says": 180, "SSIS says": 180, "agree": 145},
            desc="One row per rejected record, with BOTH engines' verdicts on "
                 "it. Disagreements sort to the top, so if the first row is "
                 "green there are none. '— loaded —' means that engine "
                 "accepted the record. Green is both engines reaching the same "
                 "conclusion, amber is one rejecting what the other kept, red "
                 "is both rejecting for different reasons. `make compare` "
                 "prints the same set and exits non-zero on it."),
    ],
    ["comparison", "ssis", "migration"])

# ------------------------------------------------- the SSIS engine's own ----
# The same board, drawn from the OTHER engine's tables, written into the OTHER
# repo's provisioning folder so his Grafana on :3001 looks like this one.
#
# Why bother, when the comparison dashboard already shows both? Because the two
# demos are given by two different people. He demos his engine alone and should
# not have to explain a different-looking dashboard; a reviewer flipping between
# :3000 and :3001 should be reading the same picture twice, not learning two
# layouts. Same panels, same order, same colours - only the SQL differs.
#
# His datasource is declared in deploy/grafana/provisioning/datasources as
# `Postgres` / uid `postgres_uid`; nothing here touches it.
# The TYPE string matters as much as the uid, and it has to be HIS Grafana's,
# not this one's. He runs grafana-oss 11.1.7; this repo runs 11.3.0. On 11.3
# the Postgres plugin id is "grafana-postgresql-datasource"; his own working
# board declares "postgres", and on 11.1.7 that is the id that resolves.
#
# Get it wrong and there is no error anywhere: the board provisions, the panels
# draw their titles and their frames, every query returns HTTP 200 -- and every
# panel renders completely empty, because the panel could not bind a
# datasource. `make check-dashboards` did not catch it either, because it posts
# the SQL to the API with an explicit uid instead of rendering the panel, so
# the queries passed while the board showed nothing. It now checks the type
# too; see check_dashboards.py.
DS_HIS = {"type": "postgres", "uid": "postgres_uid"}


def his(fn, *args, **kwargs):
    """Same panel helper, pointed at his warehouse."""
    kwargs["ds"] = DS_HIS
    return fn(*args, **kwargs)


ssis_health = dashboard(
    "ssis-pipeline-health", "1 - Pipeline Health (SSIS)",
    "The SSIS engine's own board, laid out exactly like the NiFi one so the "
    "two can be read side by side. Source: ssis_sim/stream_runner.py.",
    [
        # Panel for panel, position for position, the NiFi board's twin --
        # same four tiles, same two middle panels, same ledger. Only the SQL
        # and the engine colour differ. It stops one panel earlier because
        # there is no SSIS equivalent of NiFi's bulletins.
        his(stat, "Orders loaded",
            "SELECT count(DISTINCT order_id) AS v FROM dw.fact_sales", 0, 0,
            w=6, color=ENGINE_SSIS,
            desc="Distinct orders in the warehouse fact table."),
        his(stat, "Records rejected",
            "SELECT count(*) AS v FROM control.dlq_errors", 6, 0, w=6,
            steps=[{"color": GOOD, "value": None}, {"color": WARN, "value": 1}],
            desc="Rows in the DLQ, across all reject reasons. A count, not a "
                 "rate -- it only ever grows."),
        his(stat, "Reject rate", (
            "SELECT CASE WHEN coalesce(sum(lines_in), 0) = 0 THEN 0"
            " ELSE round(100.0 * sum(lines_rejected) / sum(lines_in), 2) END"
            " AS v FROM control.stream_file"), 12, 0, w=6, unit="percent",
            steps=[{"color": GOOD, "value": None},
                   {"color": WARN, "value": 25}, {"color": CRITICAL, "value": 90}],
            desc="Rejected lines as a share of every line read. THIS is the "
                 "health number, not the count beside it. Same thresholds as "
                 "the NiFi board so the two colours mean the same thing."),
        his(stat, "Failed files",
            "SELECT count(*) AS v FROM control.stream_file "
            "WHERE status <> 'PASS'", 18, 0, w=6,
            steps=[{"color": GOOD, "value": None},
                   {"color": CRITICAL, "value": 1}],
            desc="Files this engine could not process cleanly."),

        his(timeseries, "Ingest throughput", (
            "SELECT date_trunc('minute', load_timestamp) AS time,"
            " count(*) AS \"lines loaded\""
            " FROM dw.fact_sales WHERE $__timeFilter(load_timestamp)"
            " GROUP BY 1 ORDER BY 1"), 0, 4, w=12, h=8,
            colors={"lines loaded": ENGINE_SSIS},
            desc="Lines landing in the warehouse per minute."),

        # The series column MUST be named `metric` where a query returns one:
        # Grafana's Postgres datasource uses that name to label a series, and
        # anything else gets prefixed with the value column ("n BAD_CURRENCY").
        his(barchart, "Why records were rejected", (
            "SELECT error_type AS reason, count(*) AS records"
            " FROM control.dlq_errors GROUP BY 1 ORDER BY 2 DESC"),
            12, 4, w=12, h=8, color=QUALITY,
            desc="The reject vocabulary, as this engine reports it. These "
                 "reason names are shared verbatim with the NiFi engine - a "
                 "spelling difference would read as a behavioural one."),

        his(table, "File ledger - one row per input file", (
            "SELECT processed_at, source_file, status, lines_in, lines_clean,"
            " lines_rejected, orders_loaded, revenue"
            " FROM control.stream_file ORDER BY processed_at DESC LIMIT 25"),
            0, 12, w=24, h=9,
            overrides=LEDGER_OVERRIDES, widths=LEDGER_WIDTHS,
            desc="This engine's job ledger. Every file that arrived leaves a "
                 "row. The NiFi board draws the same eight columns from its "
                 "job_runs table, so the two line up row against row."),

        # The NiFi board's row 4, column for column. `line_no` and the value
        # columns live inside the JSONB payload here rather than in their own
        # columns, so they are lifted out -- except on DUP_KEY rows, where his
        # dlq_insert stores only {"order_id": ...} and they come back blank.
        # That is his open item 4 in spec/SSIS-CHANGES.md, not a query bug.
        his(table, "Rejection log - every record this engine refused", (
            "SELECT date_trunc('second', detected_at AT TIME ZONE 'UTC') AS at,"
            " order_id,"
            " raw_payload->>'line_no'    AS line_no,"
            " raw_payload->>'sku'        AS sku,"
            " raw_payload->>'qty'        AS qty,"
            " raw_payload->>'unit_price' AS unit_price,"
            " raw_payload->>'currency'   AS currency,"
            " error_type AS reason, error_detail AS detail"
            " FROM control.dlq_errors"
            " ORDER BY detected_at DESC, dlq_id DESC LIMIT 25"),
            0, 21, w=24, h=9, overrides=REJECT_OVERRIDES, widths=REJECT_WIDTHS,
            desc="The record, the reason, and the offending value -- newest "
                 "first. Same nine columns as the NiFi board, so the two "
                 "tables can be read row against row.\n\n"
                 "All of it, in a shell:\n"
                 "  make errors           both engines, with a reason diff\n"
                 "  docker exec -it ssis-warehouse psql -U etl_user -d etl_db\n"
                 "      SELECT * FROM control.dlq_errors ORDER BY dlq_id DESC;\n\n"
                 "For the engine's own errors: `docker logs -f ssis-engine`, "
                 "or the control.etl_run_log table."),
    ],
    ["ssis", "health", "migration"])

# --------------------------------------------- and his live board, the twin --
# This replaces a hand-written board of his that had grown to 21 panels with
# real duplication in it: queue depth appeared three times (a tile, a gauge and
# a trend line), the reject rate twice, and two chart titles promised a
# comparison their query never made -- "Lines in vs rejected per minute"
# plotted only lines_in, "Accepted vs rejected rows per minute" only accepted.
# Its "last 30 package results" table was LIMIT 3.
#
# The original is kept at deploy/grafana/ssis_stream.json.pre-mirror -- outside
# the provisioning folder, so Grafana does not load it, but one `cp` from being
# back. Everything it showed that is not here is still on his Pipeline Health
# board (reject rate, failed files, the file ledger) or one psql away.
#
# The uid is unchanged, so any link or bookmark to ssis-stream-live still opens.
ssis_business = live_board(
    "ssis-stream-live", "2 - Business Live (SSIS)", ENGINE_SSIS, DS_HIS, {
        "orders": "SELECT count(DISTINCT order_id) AS v FROM dw.fact_sales",
        "lines": "SELECT count(*) AS v FROM dw.fact_sales",
        "revenue":
            "SELECT round(coalesce(sum(total_price),0)) AS v FROM dw.fact_sales",
        "alerts": "SELECT count(*) AS v FROM control.stream_alerts",
        # date_trunc rather than $__timeGroupAlias, matching the queries that
        # already render on his Grafana 11.1.7.
        "rate": (
            "SELECT date_trunc('minute', load_timestamp) AS time,"
            " count(DISTINCT order_id) AS \"orders\", count(*) AS \"lines\""
            " FROM dw.fact_sales WHERE $__timeFilter(load_timestamp)"
            " GROUP BY 1 ORDER BY 1"),
        "revenue_rate": (
            "SELECT date_trunc('minute', load_timestamp) AS time,"
            " sum(total_price) AS \"revenue\" FROM dw.fact_sales"
            " WHERE $__timeFilter(load_timestamp) GROUP BY 1 ORDER BY 1"),
        "alerts_log": (
            "SELECT date_trunc('second', alert_ts) AS at, alert_type,"
            " order_id, customer_id, round(alert_value) AS value,"
            " detail::text AS detail FROM control.stream_alerts"
            " ORDER BY alert_ts DESC, alert_id DESC LIMIT 25"),
    }, DRILL_HIS)


HIS_DASHBOARDS = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "source",
    "deploy", "grafana", "provisioning", "dashboards")) + "/"

TARGETS = [
    ("grafana/dashboards/pipeline-health.json", health),
    ("grafana/dashboards/business-live.json", business),
    ("grafana/dashboards/engine-comparison.json", comparison),
    # written into the OTHER repo - see SSIS-CHANGES.md. These two are the
    # mirrors of the two above them, panel for panel and position for
    # position; that is the whole point of generating them from one file.
    (HIS_DASHBOARDS + "ssis_pipeline_health.json", ssis_health),
    (HIS_DASHBOARDS + "ssis_stream.json", ssis_business),
]

def output_columns(sql):
    """The column names a SELECT will produce, in order.

    Depth-aware, so a subquery's commas and a function call's arguments do not
    split the list. Good enough for the SQL in this file, and it only has to be
    good enough to notice that two boards disagree.
    """
    low = sql.lower()
    start = low.index("select ") + 7
    depth, end = 0, len(sql)
    for i in range(start, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
        elif depth == 0 and low[i:i + 6] == " from ":
            end = i
            break
    parts, depth, buf = [], 0, ""
    for ch in sql[start:end]:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(buf)
            buf = ""
        else:
            buf += ch
    parts.append(buf)

    names = []
    for part in parts:
        part = part.strip()
        low_part = part.lower()
        # Grafana's own macro, which expands to `... AS "time"` before the
        # query is sent. Without this the two throughput panels look different
        # to the check while producing identically-named columns.
        if low_part.startswith("$__timegroupalias("):
            names.append("time")
            continue
        if " as " in low_part:                       # explicit alias wins
            part = part[low_part.rindex(" as ") + 4:].strip()
        names.append(part.strip('"').split(".")[-1].strip())
    return names


# The pairs must stay identical, and "identical" is not just the grid.
#
# The first version of this compared only panel TYPE and gridPos, and passed a
# pair whose third row was `ETL job runs` with finished_at / batch_id /
# duration_ms / reject_pct / error_text on one board and `File ledger` with
# processed_at / source_file / lines_in / lines_clean / lines_rejected /
# revenue on the other. Same type, same position, eight different headers --
# two boards that could not be read as one picture, passing a mirror check.
#
# So it now compares what a reader actually sees: the panel type, where it
# sits, what it is CALLED, and the columns it produces.
for _left, _right in [(health, ssis_health), (business, ssis_business)]:
    def _shape(d):
        out = []
        for panel in d["panels"]:
            sql = panel["targets"][0]["rawSql"]
            out.append((panel["type"], tuple(sorted(panel["gridPos"].items())),
                        panel["title"], tuple(output_columns(sql))))
        return out

    left, right = _shape(_left), _shape(_right)
    if left != right:
        lines = [f"REFUSING to write: '{_left['title']}' and "
                 f"'{_right['title']}' are a mirrored pair but they differ."]
        for i in range(max(len(left), len(right))):
            a = left[i] if i < len(left) else None
            b = right[i] if i < len(right) else None
            if a == b:
                continue
            lines.append(f"  panel {i + 1}:")
            if a is None or b is None:
                lines.append(f"    one board has no panel here")
                continue
            if a[0] != b[0] or a[1] != b[1]:
                lines.append(f"    type/position  {a[0]}{a[1][0]} vs {b[0]}{b[1][0]}")
            if a[2] != b[2]:
                lines.append(f"    title    {a[2]!r}")
                lines.append(f"       vs    {b[2]!r}")
            if a[3] != b[3]:
                lines.append(f"    columns  {list(a[3])}")
                lines.append(f"       vs    {list(b[3])}")
        raise SystemExit("\n".join(lines))

for path, dash in TARGETS:
    if not os.path.isdir(os.path.dirname(path)):
        print(f"skipped {path}: directory not present")
        continue
    with open(path, "w") as fh:
        json.dump(dash, fh, indent=2)
    print(f"wrote {path}: {len(dash['panels'])} panels")
