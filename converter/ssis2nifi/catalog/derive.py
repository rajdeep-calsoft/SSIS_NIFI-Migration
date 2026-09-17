"""Turning raw SSIS properties into the handful of values a recipe needs.

This is converter knowledge, not parse knowledge, which is why it lives here
and not in `dtsx/`. "`SqlCommand` is a reference query, and the table it reads
is what `dbrecord-lookup-table-name` wants" is a statement about NiFi. The
parser's job was only to report that a property called `SqlCommand` exists and
what it contains.

Everything derived here is recorded on the component as `derived`, so the IR
shows both the raw property and the conclusion drawn from it. A reviewer can
therefore check the reasoning, not just the answer -- which is the difference
between an auditable tool and a black box.

WHERE SSIS PUTS THINGS, WHICH IS NOT WHERE YOU EXPECT
-----------------------------------------------------
A Lookup's join is NOT a component property. `JoinToReferenceColumn` at
component level is the empty string. The real join lives on the *input column*
(`CurrencyID` carries `JoinToReferenceColumn = CurrencyAlternateKey`), and the
returned columns are marked on *output columns* with `CopyFromReferenceColumn`.
Reading only component properties yields an empty join and a lookup that cannot
be generated -- silently, because the property exists and is simply blank.
"""

from __future__ import annotations

import json
import pathlib
import re

import yaml

from . import expr, script_pattern
from .support import classify
from ..ir.model import Component, Package

_TYPES = yaml.safe_load(
    (pathlib.Path(__file__).resolve().parents[2] / "catalogue" / "types.yml").read_text()
)


def avro_type(ssis_type: str) -> str:
    """SSIS type (short name or OLE DB numeric code) -> Avro type name."""
    key = (ssis_type or "").strip()
    if key in _TYPES["numeric_codes"]:
        key = _TYPES["numeric_codes"][key]
    return _TYPES["short"].get(key, {}).get("avro", "string")

# `select * from (select * from [dbo].[DimCurrency]) as refTable where ...`
# The table is the innermost FROM. Brackets are SQL Server quoting.
_FROM = re.compile(r"from\s+((?:\[[^\]]+\]|[\w]+)(?:\s*\.\s*(?:\[[^\]]+\]|[\w]+))*)", re.I)


def _unbracket(name: str) -> str:
    return "".join(part.strip().strip("[]") for part in name.split("."))


def _split_table(qualified: str) -> tuple[str, str]:
    """`[dbo].[DimCurrency]` -> ("dbo", "DimCurrency"); bare name -> ("", name)."""
    parts = [p.strip().strip("[]") for p in re.split(r"\.\s*", qualified.strip()) if p.strip()]
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return "", (parts[-1] if parts else "")


def _lookup(comp: Component) -> dict:
    props = comp.properties
    sql = props.get("SqlCommand", "")

    # Innermost FROM: the reference table. A filtered reference set is a real
    # semantic difference, since DatabaseRecordLookupService reads the whole
    # table -- so it is flagged rather than quietly dropped.
    tables = _FROM.findall(sql)
    schema, table = _split_table(tables[-1]) if tables else ("", "")
    filtered = bool(re.search(r"\bwhere\b", sql, re.I))

    joins = [
        (col.name, col.properties.get("JoinToReferenceColumn", ""))
        for port in comp.inputs
        for col in port.columns
        if col.properties.get("JoinToReferenceColumn")
    ]
    returns = [
        col.properties.get("CopyFromReferenceColumn", "")
        for port in comp.outputs
        for col in port.columns
        if col.properties.get("CopyFromReferenceColumn")
    ]

    # CacheType: 0 full, 1 partial, 2 none. NiFi has full or bounded, not partial.
    cache = {"0": "5000", "1": "5000", "2": "0"}.get(props.get("CacheType", "0"), "5000")

    return {
        "reference_schema": schema,
        "reference_table": f"{schema}.{table}" if schema else table,
        "reference_filtered": filtered,
        "input_column": joins[0][0] if joins else "",
        "join_column": joins[0][1] if joins else "",
        "composite_key": len(joins) > 1,
        "returns": returns,
        # DatabaseRecordLookupService's own "Lookup Value Columns" property,
        # confirmed against a live NiFi 1.27.0 instance's controller-service
        # descriptor: restricts which columns the reference row even
        # contains, so the recipe's result-record-path: "/" merge can only
        # ever add/overwrite the columns CopyFromReferenceColumn actually
        # asked for -- never a same-named column the reference table happens
        # to also have (e.g. both an order line and its product row have
        # unit_price). Left unset (falsy) when there is nothing to restrict
        # to, matching this component's previous, unrestricted behaviour.
        "returns_csv": ",".join(returns),
        "cache_size": cache,
        "no_match_fails": props.get("NoMatchBehavior", "0") == "0",
        # The record pipeline carries dates as strings (types.yml policy), so a
        # lookup whose key is a date column compares string to date in SQL.
        # SQL Server casts implicitly; Postgres does not.
        "date_key": any(
            _TYPES["short"].get(
                _TYPES["numeric_codes"].get(col.ssis_type, col.ssis_type), {}
            ).get("avro") == "string"
            and _TYPES["short"].get(
                _TYPES["numeric_codes"].get(col.ssis_type, col.ssis_type), {}
            ).get("format", "").startswith("yyyy")
            for port in comp.inputs for col in port.columns
            if col.properties.get("JoinToReferenceColumn")
        ),
    }


def _oledb_destination(comp: Component) -> dict:
    props = comp.properties
    schema, table = _split_table(props.get("OpenRowset", ""))
    access = props.get("AccessMode", "")
    fast_load = props.get("FastLoadOptions", "")
    return {
        "target_schema": schema,
        "target_table": table,
        "target_qualified": f"{schema}.{table}" if schema else table,
        # AccessMode 3/4 are the fast-load paths; anything else is row-at-a-time.
        "batch_size": "500" if access in {"3", "4"} else "1",
        "no_check_constraints": bool(fast_load) and "CHECK_CONSTRAINTS" not in fast_load,
        "columns": [
            col.name
            for port in comp.inputs
            for col in port.columns
            if col.name
        ],
    }


# `_x000D__x000A_` is SSIS's escaping for CR LF inside an XML attribute.
_ESCAPES = {
    "_x000D_": "\r", "_x000A_": "\n", "_x0009_": "\t",
    "_x003C_": "<", "_x003E_": ">", "_x007C_": "|", "_x0020_": " ",
}


def unescape(value: str) -> str:
    for token, char in _ESCAPES.items():
        value = value.replace(token, char)
    return value


def _flat_file_source(comp: Component, pkg: Package) -> dict:
    """Delimiters, which SSIS stores in two places that can disagree.

    The connection manager has a RowDelimiter, and each column has a
    ColumnDelimiter -- with the LAST column's delimiter actually being the row
    delimiter. In the corpus RowDelimiter is empty and the final column carries
    CR LF, so trusting RowDelimiter gives "" and a reader that treats the whole
    file as one row. NiFi's CSVReader takes a single value separator, so a file
    whose columns disagree cannot be read faithfully and is flagged.
    """
    cm_ref = next(iter(comp.connections.values()), "")
    cm = next((c for c in pkg.connections if c.ref_id == cm_ref), None)
    cm_props = cm.properties if cm else {}

    # Delimiters come from the CONNECTION MANAGER's FlatFileColumns, not from
    # the component's output columns -- the component only names the fields.
    delimiters = [
        unescape(col.get("ColumnDelimiter", ""))
        for col in (cm.columns if cm else [])
        if col.get("ColumnDelimiter")
    ]
    row_delim = unescape(cm_props.get("RowDelimiter", ""))
    inferred = False
    if not row_delim and delimiters:
        row_delim, inferred = delimiters[-1], True

    body = [d for d in delimiters[:-1]] if len(delimiters) > 1 else delimiters
    distinct = {d for d in body if d}

    # HeaderRowDelimiter says what a header row WOULD end with, not that one
    # exists. The real signal is HeaderRowsToSkip, and it is absent from every
    # package in the corpus -- so these files have no header. Inferring a
    # header from the delimiter drops the first row of real data.
    skip = cm_props.get("HeaderRowsToSkip", "0")
    has_header = skip.isdigit() and int(skip) > 0
    qualifier = unescape(cm_props.get("TextQualifier", ""))

    # With no header there is no other source of column names, so the schema
    # comes from the connection manager. Without it a reader would name the
    # fields after the first row of data.
    columns = [
        {"name": col.get("ObjectName", ""), "type": avro_type(col.get("DataType", ""))}
        for col in (cm.columns if cm else [])
        if col.get("ObjectName")
    ]

    return {
        "column_delimiter": next(iter(distinct), ","),
        "per_column_delimiters": len(distinct) > 1,
        "row_delimiter": row_delim,
        "row_delimiter_inferred": inferred,
        "has_header": "true" if has_header else "false",
        "columns": columns,
        "avro_schema": json.dumps({
            "type": "record",
            "name": "flatfile",
            "fields": [{"name": c["name"], "type": ["null", c["type"]]} for c in columns],
        }),
        "charset": "windows-1252" if cm_props.get("CodePage") == "1252" else "UTF-8",
        # SSIS writes the literal "<none>" (escaped) when there is no qualifier.
        "text_qualifier": "" if qualifier in ("", "<none>") else qualifier,
    }


def _conditional_split(comp: Component) -> dict:
    """One QueryRecord query per branch, SSIS's `Order` breaking ties, and the
    default output re-derived as "none of the named branches matched" -- the
    same mutually-exclusive-queries technique NIFI-FLOW's build_flow.py uses
    for its own reject rules (see spec/pipeline.yml), because QueryRecord has
    no built-in "else": every dynamic property is an independent WHERE
    against the same input, not a sequential if/elif chain.
    """
    conditions: dict[str, str] = {}   # port name -> bare boolean SQL condition
    ordered: list[tuple[int, str]] = []
    default_name: str | None = None
    for port in comp.outputs:
        if port.is_error_out:
            continue
        fexpr = port.properties.get("FriendlyExpression", "")
        if not fexpr:
            if default_name is not None:
                raise expr.ExprUnsupported(
                    f"ConditionalSplit {comp.name!r} has more than one output "
                    "with no expression (expected exactly one default output)"
                )
            default_name = port.name
            continue
        order = int(port.properties.get("Order", len(ordered)))
        ordered.append((order, port.name))
        conditions[port.name] = expr.translate(fexpr)

    ordered.sort()
    if default_name:
        negated = " AND ".join(f"NOT ({conditions[name]})" for _, name in ordered)
        conditions[default_name] = negated or "1=1"

    # QueryRecord's dynamic properties are full statements, not bare WHERE
    # clauses -- caught live: NiFi rejected a bare boolean expression as
    # "Non-query expression encountered in illegal context".
    branches = {name: f"SELECT * FROM FLOWFILE WHERE {cond}" for name, cond in conditions.items()}
    return {"branches": branches, "default_output": default_name or ""}


def _derived_column(comp: Component) -> dict:
    """One QueryRecord query adding every new column this component defines.

    Only ADDING new columns is supported, not replacing an existing one in
    place -- the corpus this converter targets never does the latter, and a
    faithful replace needs real type inference (see catalog/expr.py).
    """
    if len(comp.outputs) != 1:
        raise expr.ExprUnsupported(
            f"DerivedColumn {comp.name!r}: expected exactly one output port, "
            f"found {len(comp.outputs)}"
        )
    port = comp.outputs[0]
    additions = [
        f"{expr.translate(col.properties['FriendlyExpression'])} AS {col.name}"
        for col in port.columns
        if col.properties.get("FriendlyExpression")
    ]
    if not additions:
        raise expr.ExprUnsupported(f"DerivedColumn {comp.name!r} defines no expressions")

    # Same shape as _conditional_split's `branches`: {output port name ->
    # query}, one entry here. Sharing the shape means DerivedColumn reuses
    # the identical `properties_from` / `outputs: "@dynamic:main"` recipe
    # pattern rather than needing one of its own.
    query = "SELECT *, " + ", ".join(additions) + " FROM FLOWFILE"
    return {"branches": {port.name: query}}


def _sort(comp: Component) -> dict:
    """Sort's dedup-only safe case (support.py's `_sort_is_dedup_only` already
    gated this) -> one QueryRecord query using ROW_NUMBER() OVER (PARTITION BY
    ...), the exact technique proven in destination/generator/gen/build_flow.py's
    own "8b. Dedupe" processor, a live, already-running NiFi flow -- not
    invented here. Row order beyond "first key occurrence wins" is not
    preserved; that is what makes this only the dedup case, not a general Sort.
    """
    keyed = sorted(
        ((int(c.properties["sortKeyPosition"]), c.name)
         for inp in comp.inputs for c in inp.columns
         if c.properties.get("sortKeyPosition")),
    )
    keys = ", ".join(name for _, name in keyed)
    query = (
        "SELECT * FROM (SELECT FLOWFILE.*, ROW_NUMBER() OVER ("
        f"PARTITION BY {keys} ORDER BY {keys}) AS dup_rank FROM FLOWFILE) t "
        "WHERE dup_rank = 1"
    )
    return {"branches": {comp.outputs[0].name: query}}


_AGGREGATE_FUNCS = {"sum": "SUM", "min": "MIN", "max": "MAX", "count": "COUNT"}


def _aggregate(comp: Component) -> dict:
    """Aggregate's safe GROUP BY case (support.py's `_aggregate_is_simple_groupby`
    already gated this) -> one QueryRecord GROUP BY query, the same MAX/SUM
    shape proven in destination/generator/gen/build_flow.py's own
    "8. Aggregate orders + alert rules" processor, a live, already-running
    NiFi flow -- not invented here.
    """
    out = comp.outputs[0]
    group_cols = [c.name for c in out.columns
                  if c.properties.get("AggregationType", "").lower() == "groupby"]
    selects = list(group_cols)
    for c in out.columns:
        agg = c.properties.get("AggregationType", "").lower()
        if agg == "groupby":
            continue
        func = _AGGREGATE_FUNCS[agg]
        source = c.properties.get("SourceColumn", "")
        arg = source if source else ("*" if func == "COUNT" else c.name)
        selects.append(f"{func}({arg}) AS {c.name}")
    query = f"SELECT {', '.join(selects)} FROM FLOWFILE GROUP BY {', '.join(group_cols)}"
    return {"branches": {out.name: query}}


def _script_component(comp: Component) -> dict:
    """Script Component's one recognised idiom (support.py's
    `_script_is_get_error_description` already gated this, using the same
    script_pattern.recognise_get_error_description() call) -> the column
    names a LookupRecord-against-a-static-catalogue recipe needs.
    """
    out_col, in_col = script_pattern.recognise_get_error_description(
        comp.properties["SourceCode_files"]
    )
    return {"error_code_column": in_col, "error_description_column": out_col}


_DERIVERS = {
    "Microsoft.Lookup": lambda c, p: _lookup(c),
    "Microsoft.OLEDBDestination": lambda c, p: _oledb_destination(c),
    "Microsoft.FlatFileSource": _flat_file_source,
    "Microsoft.ConditionalSplit": lambda c, p: _conditional_split(c),
    "Microsoft.DerivedColumn": lambda c, p: _derived_column(c),
    "Microsoft.Sort": lambda c, p: _sort(c),
    "Microsoft.Aggregate": lambda c, p: _aggregate(c),
    "Microsoft.ManagedComponentHost": lambda c, p: _script_component(c),
    "Microsoft.ScriptComponentHost": lambda c, p: _script_component(c),
}


def derive(pkg: Package) -> Package:
    """Attach a `derived` block to every component we know how to read.

    A _DERIVERS entry for a NARROW class_id (Sort, Aggregate, the script
    hosts) assumes classify()'s predicate already held -- e.g. _aggregate()
    reads AggregationType values classify() already checked are all in the
    safe set. Calling it on an instance that DIDN'T pass gets a bare
    KeyError/IndexError, not a diagnostic. So classify() is consulted here
    first, the same single source of truth report/graph.py uses: only a
    "supported" verdict gets derived; anything "refused" or "unknown" is left
    `derived`-less, same as an unrecognised class_id.

    An expr.ExprUnsupported is caught here too, not left to crash the CLI: it
    is reported the same way an unsupported component is (catalog/support.py)
    -- a loud, specific diagnostic, with the component left `derived`-less so
    flowdef.build()'s `requires` check refuses it explicitly rather than
    emitting a flow with a silently wrong predicate.
    """
    for df in pkg.dataflows:
        for comp in df.components:
            fn = _DERIVERS.get(comp.class_id)
            if fn is None:
                continue
            verdict, _ = classify(comp)
            if verdict != "supported":
                continue
            try:
                comp.derived = fn(comp, pkg)
            except expr.ExprUnsupported as exc:
                pkg.diag("error", "EXPR_UNSUPPORTED", str(exc), node=comp.id)
    return pkg
