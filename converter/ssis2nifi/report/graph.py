"""Render a parsed package as something a human can check in one screen.

The audience is a person who owns the SSIS package and does not know NiFi.
The question they need to answer is "is this what my package does?", so the
vocabulary here stays entirely SSIS's own -- component names, output names,
dispositions -- and never mentions a processor.

Unconnected outputs are printed with the consequence spelled out rather than
the flag value, because `NoMatchBehavior=0` means nothing to a reader and
"fails the data flow" means everything.
"""

from __future__ import annotations

from ..dtsx import dialect
from ..catalog.support import classify
from ..ir.model import Component, Package

TICK, CROSS, WARN = "✓", "✗", "!"

_CONSEQUENCE = {
    "fail_component": "fails the data flow on a miss",
    "redirect": "rows would be redirected here",
    "unknown": "unconnected",
}


def _status(comp: Component) -> tuple[str, str]:
    verdict, reason = classify(comp)
    return (TICK, "") if verdict == "supported" else (CROSS, reason)


def render(pkg: Package, show_graph: bool = True) -> str:
    L: list[str] = []
    add = L.append

    add(f"package: {pkg.name}   ({pkg.creation_name or 'unknown'}"
        + (f", format {pkg.package_format_version}" if pkg.package_format_version else "")
        + f", {pkg.dialect} dialect)")

    kinds: dict[str, int] = {}
    for c in pkg.connections:
        kinds[c.kind] = kinds.get(c.kind, 0) + 1
    conn_desc = ", ".join(f"{n} {k}" for k, n in sorted(kinds.items())) or "none"
    add(f"connections: {len(pkg.connections)} ({conn_desc})")
    add(f"control flow: {len(pkg.tasks)} task(s), {len(pkg.precedence)} precedence constraint(s)")
    if pkg.variables:
        add(f"variables: {len(pkg.variables)}")

    for df in pkg.dataflows:
        add("")
        add(f'dataflow "{df.name}": {len(df.components)} components, {len(df.edges)} paths')
        if not show_graph:
            continue

        out_edges: dict[str, list] = {}
        for e in df.edges:
            out_edges.setdefault(e.from_node, []).append(e)
        names = {c.id: c.name for c in df.components}
        dangling = {(d.node, d.output): d for d in df.dangling_outputs}

        add("")
        for comp in df.components:
            mark, why = _status(comp)
            head = f"  [{comp.name}]  {dialect.short(comp.raw_class_id) or comp.raw_class_id}"
            add(f"{head}  {mark}" + (f"  -- {why}" if why else ""))

            ports = comp.outputs
            for i, port in enumerate(ports):
                last = i == len(ports) - 1
                stem = "   └─" if last else "   ├─"
                targets = [e for e in out_edges.get(comp.id, []) if e.from_output == port.name]
                if targets:
                    dest = ", ".join(f"[{names.get(e.to_node, e.to_node)}]" for e in targets)
                    add(f"{stem} {port.name} ──▶ {dest}")
                else:
                    d = dangling.get((comp.id, port.name))
                    note = _CONSEQUENCE.get(d.disposition, "unconnected") if d else "unconnected"
                    add(f"{stem} {port.name}  {CROSS} {note}")

    errors = [d for d in pkg.diagnostics if d.severity == "error"]
    warns = [d for d in pkg.diagnostics if d.severity == "warn"]
    if errors or warns:
        add("")
        add("diagnostics:")
        for d in errors + warns:
            tag = CROSS if d.severity == "error" else WARN
            where = f" [{d.node}]" if d.node else ""
            add(f"  {tag} {d.code}{where}: {d.message}")

    c = pkg.coverage
    add("")
    add(f"coverage: {c.recognised}/{c.total_components} convertible, "
        f"{c.unsupported} need manual review")
    return "\n".join(L)
