"""Read a .dtsx into the IR.

This module knows about SSIS and XML. It knows nothing about NiFi -- that
boundary is enforced by tests/unit/test_layering.py, and it is what makes the
IR reviewable by someone who has never seen NiFi: you can hand them the IR and
ask "is this what your package does?".

NAMESPACE NOTE, AND WHY IT IS A CORRECTNESS CHECK
-------------------------------------------------
Genuine packages use the namespace `www.microsoft.com/SqlServer/Dts` -- with no
scheme.  Inside `DTS:ObjectData`, the `<pipeline>` element and everything under
it (`components`, `paths`, `component`, `output`, ...) is declared
form="unqualified", so those are matched WITHOUT a namespace.  Mixing the two
up is the single most common way to write a DTSX parser that silently finds
nothing, so both spellings are constants here rather than inline strings.

A file that declares `http://www.microsoft.com/SqlServer/Dts`, or that has
components with an empty `componentClassID` and no `<paths>`, is not a package
Visual Studio produced.  We refuse it loudly instead of half-parsing it.
"""

from __future__ import annotations

import hashlib
import os
import xml.etree.ElementTree as ET

from . import dialect, refid
from ..ir.model import (
    Component,
    ConnectionManager,
    DanglingOutput,
    DataFlow,
    Edge,
    OutputColumn,
    Package,
    Port,
    PrecedenceConstraint,
    Task,
)

DTS = "{www.microsoft.com/SqlServer/Dts}"
# The wrong one. Seen only in hand-generated files that imitate DTSX.
DTS_BOGUS = "{http://www.microsoft.com/SqlServer/Dts}"

PIPELINE_TYPES = {"Microsoft.Pipeline", "SSIS.Pipeline", "SSIS.Pipeline.2", "SSIS.Pipeline.3"}
SQLTASK_TYPES = {"Microsoft.ExecuteSQLTask", "STOCK:SQLTask"}
CONTAINER_TYPES = {"STOCK:FOREACHLOOP", "STOCK:FORLOOP", "STOCK:SEQUENCE"}



class NotADtsxPackage(Exception):
    """The file is not a genuine SSIS package. Refuse rather than half-parse."""


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _slug(ref: str) -> str:
    """A stable, readable id for reports. The refId remains the real identity."""
    leaf = refid.parse(ref).leaf or ref
    out = "".join(c.lower() if c.isalnum() else "_" for c in leaf)
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_") or "node"


def _props(el: ET.Element | None, ns: str = "") -> dict[str, str]:
    """`<properties><property name="X">v</property></properties>` -> {X: v}."""
    if el is None:
        return {}
    out: dict[str, str] = {}
    for p in el.findall(f"{ns}property"):
        name = p.get(f"{ns}name") or p.get("name")
        if name:
            out[name] = (p.text or "").strip()
    return out


def _dts_props(el: ET.Element) -> dict[str, str]:
    """Root/executable `DTS:Property` children -> dict."""
    out: dict[str, str] = {}
    for p in el.findall(f"{DTS}Property"):
        name = p.get(f"{DTS}Name")
        if name:
            out[name] = (p.text or "").strip()
    return out


def _output_semantics(name: str, is_error: bool) -> str:
    """What an output *means*, from its name -- SSIS's own branch vocabulary."""
    if is_error:
        return "error"
    low = name.lower()
    if "no match" in low:
        return "no_match"
    if "match" in low:
        return "match"
    return "success"


def _disposition(comp: Component, port: Port) -> str:
    """What SSIS does when this output would receive a row but nothing is wired.

    This is the field that decides whether the generated NiFi flow drops rows
    silently.  `NoMatchBehavior=0` on a Lookup means *fail the data flow*; a
    NiFi `unmatched` relationship auto-terminated would instead discard them.
    """
    if port.semantics == "no_match":
        # 0 = fail the component, 1 = send rows to the no-match output
        return "fail_component" if comp.properties.get("NoMatchBehavior", "0") == "0" else "redirect"
    if port.semantics == "error":
        return "redirect"
    return "unknown"


def _columns(port_el: ET.Element, tag: str) -> list[OutputColumn]:
    """Columns of one port.

    Output and input columns are spelled differently, and the difference is not
    cosmetic.  An OUTPUT column is the definition of a field, so it carries
    `name` and `dataType`.  An INPUT column is a *reference* to a field defined
    upstream, so its identity is `lineageId` and its human-readable name is only
    cached (`cachedName`, `cachedDataType`) for the designer's benefit.

    Reading only `name` therefore yields empty strings for every input column --
    silently, because the attribute is simply absent rather than wrong.
    `externalMetadataColumnId` is kept too: for a destination it is the column
    in the target table, which is what a column map has to be built from.
    """
    cols: list[OutputColumn] = []
    holder = port_el.find(tag)
    if holder is None:
        return cols
    for c in holder:
        cols.append(
            OutputColumn(
                name=c.get("name") or c.get("cachedName", ""),
                ssis_type=c.get("dataType") or c.get("cachedDataType", ""),
                ref_id=c.get("refId", ""),
                lineage_id=c.get("lineageId", ""),
                external_metadata_id=c.get("externalMetadataColumnId", ""),
                error_row_disposition=c.get("errorRowDisposition", ""),
                truncation_row_disposition=c.get("truncationRowDisposition", ""),
                properties=_props(c.find("properties")),
            )
        )
    return cols


def _source_code_files(properties_el: ET.Element | None) -> dict[str, str]:
    """A Script Component's `SourceCode` property is `isArray="true"`, not
    plain text -- `_props()` only reads `.text`, which is empty for an array
    property, so this reads it separately. The array is a flat sequence of
    (filename, encoding, content) triples, one per file in the script
    project; this returns {filename: content}, ignoring the encoding entries
    (this repo's corpus is UTF8 throughout).
    """
    if properties_el is None:
        return {}
    for p in properties_el.findall("property"):
        if p.get("name") != "SourceCode":
            continue
        elements = p.findall("arrayElements/arrayElement")
        texts = [(e.text or "") for e in elements]
        return {texts[i]: texts[i + 2] for i in range(0, len(texts) - 2, 3)}
    return {}


def _parse_component(el: ET.Element) -> Component:
    raw_class = el.get("componentClassID", "")
    ref = el.get("refId", "")
    comp = Component(
        id=_slug(ref),
        ref_id=ref,
        class_id=dialect.canonical(raw_class),
        raw_class_id=raw_class,
        name=el.get("name", ""),
        description=el.get("description", ""),
        uses_dispositions=el.get("usesDispositions", "") == "true",
        properties=_props(el.find("properties")),
    )
    files = _source_code_files(el.find("properties"))
    if files:
        comp.properties["SourceCode_files"] = files
    for c in el.findall("connections/connection"):
        # `name` is the component's role for the connection (e.g. OleDbConnection)
        comp.connections[c.get("name", "")] = c.get("connectionManagerRefId", "")

    for kind, tag, col_tag in (("output", "outputs/output", "outputColumns"),
                               ("input", "inputs/input", "inputColumns")):
        for p in el.findall(tag):
            is_err = p.get("isErrorOut", "") == "true"
            name = p.get("name", "")
            port = Port(
                name=name,
                ref_id=p.get("refId", ""),
                kind=kind,
                is_error_out=is_err,
                semantics=_output_semantics(name, is_err) if kind == "output" else "success",
                columns=_columns(p, col_tag),
                properties=_props(p.find("properties")),
            )
            (comp.outputs if kind == "output" else comp.inputs).append(port)
    return comp


def _parse_dataflow(task_el: ET.Element, pkg: Package) -> DataFlow | None:
    obj = task_el.find(f"{DTS}ObjectData")
    if obj is None:
        return None
    pipe = obj.find("pipeline")
    if pipe is None:
        return None

    ref = task_el.get(f"{DTS}refId", "")
    df = DataFlow(id="df." + _slug(ref), ref_id=ref, name=task_el.get(f"{DTS}ObjectName", ""))

    comps_el = pipe.find("components")
    if comps_el is None:
        raise NotADtsxPackage(f"pipeline task {ref!r} has no <components>")
    by_ref: dict[str, Component] = {}
    for c_el in comps_el.findall("component"):
        comp = _parse_component(c_el)
        df.components.append(comp)
        by_ref[comp.ref_id] = comp

    paths_el = pipe.find("paths")
    n_paths = 0 if paths_el is None else len(paths_el.findall("path"))
    if df.components and n_paths == 0 and len(df.components) > 1:
        # Multiple components and no edges at all is not a valid data flow.
        raise NotADtsxPackage(
            f"pipeline task {ref!r} has {len(df.components)} components but no <paths>; "
            "a real package always wires its components together"
        )

    wired: set[tuple[str, str]] = set()
    for i, p_el in enumerate(paths_el.findall("path") if paths_el is not None else [], 1):
        start, end = p_el.get("startId", ""), p_el.get("endId", "")
        src_ref, dst_ref = refid.component_of(start), refid.component_of(end)
        src, dst = by_ref.get(src_ref), by_ref.get(dst_ref)
        if src is None or dst is None:
            pkg.diag("error", "PATH_DANGLING_ENDPOINT",
                     f"path {p_el.get('refId', '')!r} references a component not in this data flow")
            continue
        out_name = refid.parse(start).port("Outputs") or ""
        in_name = refid.parse(end).port("Inputs") or ""
        sem = next((o.semantics for o in src.outputs if o.name == out_name), "success")
        df.edges.append(Edge(id=f"p.{i}", ref_id=p_el.get("refId", ""), from_node=src.id,
                             from_output=out_name, to_node=dst.id, to_input=in_name, semantics=sem))
        wired.add((src.id, out_name))

    # Outputs with nothing attached. See ir/model.py for why this matters.
    for comp in df.components:
        for port in comp.outputs:
            if (comp.id, port.name) not in wired:
                df.dangling_outputs.append(
                    DanglingOutput(node=comp.id, output=port.name,
                                   semantics=port.semantics, disposition=_disposition(comp, port))
                )
    return df


def _resolve_lineage(pkg: Package) -> None:
    """Point every input column at the component/output that actually produced it.

    DTSX identifies a column by the refId of its *producing* output column, and
    that producer is often not the component immediately upstream.  Without
    this map, a generated flow wires the wrong field.
    """
    producers: dict[str, dict[str, str]] = {}
    for df in pkg.dataflows:
        for comp in df.components:
            for port in comp.outputs:
                for col in port.columns:
                    if col.ref_id:
                        producers[col.ref_id] = {"node": comp.id, "output": port.name, "column": col.name}

    unresolved = 0
    for df in pkg.dataflows:
        for comp in df.components:
            for port in comp.inputs:
                for col in port.columns:
                    if not col.lineage_id:
                        continue
                    hit = producers.get(col.lineage_id)
                    if hit:
                        col.lineage_resolved = hit
                    else:
                        unresolved += 1
    if unresolved:
        pkg.diag("warn", "LINEAGE_UNRESOLVED",
                 f"{unresolved} input column(s) reference a lineageId not produced in this package")


def _walk_executables(parent: ET.Element, pkg: Package) -> None:
    """Recurse the control flow. Containers nest, so this cannot be a flat loop."""
    holder = parent.find(f"{DTS}Executables")
    if holder is None:
        return
    for ex in holder.findall(f"{DTS}Executable"):
        etype = ex.get(f"{DTS}ExecutableType", "")
        ref = ex.get(f"{DTS}refId", "")
        name = ex.get(f"{DTS}ObjectName", "") or refid.parse(ref).leaf

        if etype in PIPELINE_TYPES:
            kind = "data_flow"
        elif etype in SQLTASK_TYPES:
            kind = "execute_sql"
        elif etype in CONTAINER_TYPES:
            kind = "container"
        else:
            kind = "other"

        task = Task(id="task." + _slug(ref), ref_id=ref, name=name,
                    executable_type=etype, kind=kind,
                    disabled=_dts_props(ex).get("Disabled", "").lower() == "true")

        if kind == "data_flow":
            df = _parse_dataflow(ex, pkg)
            if df is not None:
                pkg.dataflows.append(df)
                task.dataflow = df.id
        pkg.tasks.append(task)

        # Precedence constraints are declared on the *parent* in some versions
        # and alongside the executable in others; collect from both.
        _walk_executables(ex, pkg)

    for pc in parent.findall(f"{DTS}PrecedenceConstraints/{DTS}PrecedenceConstraint"):
        pkg.precedence.append(
            PrecedenceConstraint(
                from_task="task." + _slug(pc.get(f"{DTS}From", "")),
                to_task="task." + _slug(pc.get(f"{DTS}To", "")),
                value=pc.get(f"{DTS}Value", "Success"),
                expression=pc.get(f"{DTS}Expression", ""),
            )
        )


def _connection_kind(creation_name: str) -> str:
    cn = (creation_name or "").upper()
    for token in ("OLEDB", "FLATFILE", "ADO.NET", "ADONET", "FILE", "EXCEL", "ODBC"):
        if token in cn:
            return token.replace(".", "")
    return cn or "UNKNOWN"


def parse_file(path: str) -> Package:
    """Parse a .dtsx into a Package, or raise NotADtsxPackage."""
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise NotADtsxPackage(f"{os.path.basename(path)} is not well-formed XML: {exc}") from exc

    if root.tag.startswith(DTS_BOGUS):
        raise NotADtsxPackage(
            f"{os.path.basename(path)} declares namespace 'http://www.microsoft.com/SqlServer/Dts'. "
            "Genuine packages use 'www.microsoft.com/SqlServer/Dts' with no scheme. "
            "This file was not produced by SQL Server Data Tools."
        )
    if root.tag != f"{DTS}Executable":
        raise NotADtsxPackage(f"{os.path.basename(path)}: root element is {root.tag!r}, expected DTS:Executable")

    props = _dts_props(root)
    pkg = Package(
        source_file=os.path.abspath(path),
        sha256=_sha256(path),
        name=props.get("ObjectName", "") or root.get(f"{DTS}ObjectName", "") or os.path.basename(path),
        creation_name=root.get(f"{DTS}CreationName", "") or props.get("CreationName", ""),
        package_format_version=props.get("PackageFormatVersion", ""),
        dts_product_version=props.get("VersionBuild", ""),
    )

    for cm in root.findall(f"{DTS}ConnectionManagers/{DTS}ConnectionManager"):
        cm_props = _dts_props(cm)
        creation = cm.get(f"{DTS}CreationName", "") or cm_props.get("CreationName", "")
        ref = cm.get(f"{DTS}refId", "")
        obj = cm.find(f"{DTS}ObjectData")
        inner: dict[str, str] = {}
        columns: list[dict[str, str]] = []
        if obj is not None and len(obj):
            node = list(obj)[0]
            inner = {k.split("}")[-1]: v for k, v in node.attrib.items()}
            # A flat file's per-column delimiters live here. Without them the
            # row delimiter cannot be recovered when RowDelimiter is empty,
            # which it is in every package in the corpus.
            for col in node.findall(f"{DTS}FlatFileColumns/{DTS}FlatFileColumn"):
                columns.append({k.split("}")[-1]: v for k, v in col.attrib.items()})
        pkg.connections.append(
            ConnectionManager(id="cm." + _slug(ref), ref_id=ref, kind=_connection_kind(creation),
                              creation_name=creation, properties={**cm_props, **inner},
                              columns=columns)
        )

    for v in root.findall(f"{DTS}Variables/{DTS}Variable"):
        pkg.variables.append({
            "name": v.get(f"{DTS}ObjectName", ""),
            "namespace": v.get(f"{DTS}Namespace", ""),
            "ref_id": v.get(f"{DTS}refId", ""),
        })

    _walk_executables(root, pkg)
    _resolve_lineage(pkg)

    # Dialect + coverage, both computed from what was actually seen.
    raw = [c.raw_class_id for df in pkg.dataflows for c in df.components if c.raw_class_id]
    guids = sum(1 for r in raw if dialect.is_guid_dialect(r))
    pkg.dialect = "mixed" if 0 < guids < len(raw) else ("guid" if guids else "friendly")


    if not pkg.dataflows:
        pkg.diag("warn", "NO_DATA_FLOW", "package contains no pipeline task; nothing to convert")
    return pkg
