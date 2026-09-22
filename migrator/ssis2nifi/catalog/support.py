"""Which SSIS components this tool can convert, and why it refuses the rest.

This lives in the catalogue layer, not the parser, and the split is deliberate.

The parser's job is to say what is IN the package -- that is a fact about SSIS
and nothing else, which is why `ssis2nifi/dtsx/` is not allowed to mention NiFi
(enforced by tests/unit/test_refid_and_layering.py). Whether a component can be
converted is a different kind of claim: it is about what NiFi can express, and
it changes as the catalogue grows. Keeping it here means the IR stays a neutral
description of the customer's package, and coverage is an annotation applied to
it rather than a property baked into it.

REFUSING IS A FEATURE. A component listed in REFUSED is one we understand well
enough to know that a faithful translation does not exist. Saying so is worth
more than emitting something plausible and wrong -- the whole reason a hand-run
AI translation was rejected is that it cannot tell you where it guessed.
"""

from __future__ import annotations

from . import script_pattern
from ..ir.model import Component, Coverage, Package

# componentClassID values with a conversion recipe that applies unconditionally.
SUPPORTED: set[str] = {
    "Microsoft.FlatFileSource",
    "Microsoft.FlatFileDestination",
    "Microsoft.OLEDBSource",
    "Microsoft.OLEDBDestination",
    "Microsoft.Lookup",
    "Microsoft.DerivedColumn",
    "Microsoft.ConditionalSplit",
}

# componentClassID values with a conversion recipe that only applies to a
# narrow, safe subset of that component's real behaviour -- the general case
# stays refused. Each entry is a predicate over the parsed Component: True
# means "this specific instance is in the safe subset", checked BEFORE
# REFUSED below, so a Sort with EliminateDuplicates=true converts while a
# Sort used for pure ordering still doesn't.
#
# These recipes are best-effort: the SSIS-side property names below
# (EliminateDuplicates, sortKeyPosition, AggregationType, SourceColumn) were
# not harvested from a real SQL-Server-Data-Tools-produced .dtsx -- none
# exists anywhere in this repo to harvest from (see scripts/precision_check.py's
# "ADDABLE gaps" -- these components had zero occurrences before this recipe
# was written). They follow the well-documented, standard SSIS Sort/Aggregate
# transform shape. If a real package ever exercises this path and NiFi/the
# analyzer rejects it, that is this narrowing being wrong, not the principle.
def _sort_is_dedup_only(comp: Component) -> bool:
    """Sort's one faithfully-translatable case: EliminateDuplicates, where the
    actual downstream order doesn't matter (NiFi's QueryRecord ROW_NUMBER()
    trick, the same pattern destination/generator/gen/build_flow.py's own
    proven "8b. Dedupe" processor uses, drops a duplicate but does not
    guarantee row order the way a real Sort would)."""
    if comp.properties.get("EliminateDuplicates", "false").lower() != "true":
        return False
    keys = [c for inp in comp.inputs for c in inp.columns if c.properties.get("sortKeyPosition")]
    return bool(keys) and len(comp.outputs) == 1


_AGGREGATE_SAFE_FUNCS = {"groupby", "sum", "min", "max", "count"}


def _aggregate_is_simple_groupby(comp: Component) -> bool:
    """Aggregate's safe subset: GROUP BY with only SUM/MIN/MAX/COUNT(*), which
    QueryRecord's GROUP BY can express exactly. AVERAGE and COUNT DISTINCT are
    excluded on purpose -- NULL-handling and distinct-counting are exactly
    where SSIS and Calcite are documented to genuinely differ (see the
    now-superseded plan's M6.2 narrowing note)."""
    if len(comp.outputs) != 1:
        return False
    cols = comp.outputs[0].columns
    if not cols:
        return False
    types = [c.properties.get("AggregationType", "").lower() for c in cols]
    if any(t not in _AGGREGATE_SAFE_FUNCS for t in types):
        return False
    return "groupby" in types


def _script_is_get_error_description(comp: Component) -> bool:
    """Script Component's one safe idiom (see catalog/script_pattern.py's
    module docstring for the full argument): the ENTIRE script body must be
    exactly `Row.X = ComponentMetaData.GetErrorDescription(Row.Y)`, and both
    named columns must actually exist on this component's ports -- a name the
    bounded regex happened to match but that isn't a real column would be a
    sign this component isn't the idiom after all, not something to convert
    anyway."""
    files = comp.properties.get("SourceCode_files")
    if not files:
        return False
    result = script_pattern.recognise_get_error_description(files)
    if result is None:
        return False
    out_col, in_col = result
    has_out = any(c.name == out_col for port in comp.outputs for c in port.columns)
    has_in = any(c.name == in_col for port in comp.inputs for c in port.columns)
    return has_out and has_in


NARROW: dict[str, tuple[callable, str]] = {
    "Microsoft.Sort": (
        _sort_is_dedup_only,
        "Sort is blocking and order-changing in general; only the "
        "EliminateDuplicates case (a QueryRecord ROW_NUMBER() dedup, order not "
        "preserved) has a faithful NiFi equivalent",
    ),
    "Microsoft.Aggregate": (
        _aggregate_is_simple_groupby,
        "Aggregate is refused in general; only GROUP BY with SUM/MIN/MAX/COUNT(*) "
        "matches QueryRecord's GROUP BY exactly -- AVERAGE and COUNT DISTINCT "
        "differ between SSIS and Calcite and stay refused",
    ),
    "Microsoft.ManagedComponentHost": (
        _script_is_get_error_description,
        "Script Component is arbitrary .NET in general and stays refused; only "
        "the single recognised idiom Row.X = ComponentMetaData.GetErrorDescription"
        "(Row.Y) -- nothing else -- has a bounded, non-guessing NiFi translation, "
        "see catalog/script_pattern.py",
    ),
    "Microsoft.ScriptComponentHost": (
        _script_is_get_error_description,
        "Script Component is arbitrary .NET in general and stays refused; only "
        "the single recognised idiom Row.X = ComponentMetaData.GetErrorDescription"
        "(Row.Y) -- nothing else -- has a bounded, non-guessing NiFi translation, "
        "see catalog/script_pattern.py",
    ),
}

# Components we recognise and deliberately refuse outright, with the reason
# in NiFi terms.
REFUSED: dict[str, str] = {
    "Microsoft.MergeJoin":
        "Merge Join needs two independent sorted streams joined by key; checked "
        "live NiFi 1.27's processor catalogue for an equivalent -- JoinEnrichment "
        "exists but is shaped for fork-one-stream-and-enrich-it (paired with "
        "ForkEnrichment), not two genuinely independent upstream branches, so "
        "using it here would risk silently wrong join semantics. A Merge Join "
        "whose second input is really a reference/dimension table (the common "
        "real-world case) should be authored as Microsoft.Lookup instead, which "
        "already converts -- see Microsoft.Lookup in this file",
}

# Control-flow containers: orchestration, which NiFi has no concept of.
CONTAINER_NOTE = (
    "{kind} is orchestration; NiFi has no equivalent. Its inner tasks are converted, "
    "the looping is reported as execution order rather than generated."
)


def classify(comp: Component) -> tuple[str, str]:
    """(verdict, reason) for one component.

    verdict is supported | refused | unknown. Takes the whole component, not
    just its class_id, because Sort/Aggregate's verdict depends on which
    specific behaviour this instance uses (see NARROW above).
    """
    class_id = comp.class_id
    if class_id in SUPPORTED:
        return "supported", ""
    if class_id in NARROW:
        predicate, reason = NARROW[class_id]
        if predicate(comp):
            return "supported", ""
        return "refused", reason
    if class_id in REFUSED:
        return "refused", REFUSED[class_id]
    return "unknown", "no conversion rule; requires manual review"


def annotate(pkg: Package) -> Package:
    """Fill in coverage and the conversion diagnostics, in place.

    Called after parsing. Separating it keeps `parse_file()` a pure description
    of the package, so the same IR can be re-scored as the catalogue grows
    without re-reading the .dtsx.
    """
    cov = Coverage(total_components=sum(len(df.components) for df in pkg.dataflows))
    for df in pkg.dataflows:
        for comp in df.components:
            verdict, reason = classify(comp)
            if verdict == "supported":
                cov.recognised += 1
                continue
            cov.unsupported += 1
            pkg.diag(
                "error",
                "COMPONENT_REFUSED" if verdict == "refused" else "COMPONENT_UNKNOWN",
                f"{comp.class_id or comp.raw_class_id!r}: {reason}",
                node=comp.id,
            )

    for task in pkg.tasks:
        if task.kind == "container":
            pkg.diag("warn", "CONTROL_FLOW_CONTAINER",
                     CONTAINER_NOTE.format(kind=task.executable_type), node=task.id)

    pkg.coverage = cov
    return pkg
