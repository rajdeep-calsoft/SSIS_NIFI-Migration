"""ssis2nifi -- read a real SSIS package and say what it does.

Exit codes are part of the contract, because this runs in CI:

    0   everything in the package has a conversion rule
    3   parsed, but something needs a human (unsupported component, refused component)
    4   refused: not a genuine .dtsx, or malformed beyond use

3 is deliberately non-zero.  A pipeline must not be able to ship a
half-translated package by accident, and "it printed a warning" is not a gate.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

from .catalog.derive import derive
from .catalog.support import annotate
from .dtsx.parse import NotADtsxPackage, parse_file
from .emit import flowdef
from .ir import schema
from .report import graph

EXIT_OK, EXIT_REVIEW, EXIT_REFUSED = 0, 3, 4


def _cmd_analyze(args: argparse.Namespace) -> int:
    try:
        pkg = derive(annotate(parse_file(args.package)))
    except NotADtsxPackage as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    if args.json:
        print(json.dumps(pkg.to_dict(), indent=2, default=str))
    else:
        print(graph.render(pkg, show_graph=not args.no_graph))

    return EXIT_OK if pkg.coverage.all_recognised else EXIT_REVIEW


def _cmd_ir(args: argparse.Namespace) -> int:
    """Write the IR. This is the artifact a package owner reviews."""
    try:
        pkg = derive(annotate(parse_file(args.package)))
    except NotADtsxPackage as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    text = schema.dump(pkg)
    if args.out:
        pathlib.Path(args.out).write_text(text)
        print(f"wrote {args.out}  ({len(text.splitlines())} lines)", file=sys.stderr)
        print(f"  content  digest: {schema.content_digest(pkg)[:16]}", file=sys.stderr)
        print(f"  topology digest: {schema.topology_digest(pkg)[:16]}", file=sys.stderr)
    else:
        print(text, end="")
    return EXIT_OK if pkg.coverage.all_recognised else EXIT_REVIEW


def _cmd_convert(args: argparse.Namespace) -> int:
    """IR -> flow.json + secrets sidecar. Pure: no NiFi needed."""
    try:
        pkg = derive(annotate(parse_file(args.package)))
    except NotADtsxPackage as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    try:
        flow, secrets, notes = flowdef.build(
            pkg, flowdef.load_bindings(args.bindings), args.group_name
        )
    except flowdef.EmitError as exc:
        print(f"cannot generate: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    out = pathlib.Path(args.out or f"out/{pathlib.Path(args.package).stem}.flow.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(flow, indent=2))

    fc = flow["flowContents"]
    print(f"wrote {out}", file=sys.stderr)
    print(f"  {len(fc['processors'])} processors, {len(fc['connections'])} connections, "
          f"{len(fc['controllerServices'])} controller services", file=sys.stderr)

    if secrets:
        side = out.with_suffix("").with_suffix(".secrets.json")
        side.write_text(json.dumps(secrets, indent=2))
        print(f"  {len(secrets)} sensitive propert(ies) left null; see {side.name}", file=sys.stderr)

    for note in notes:
        print(f"  note: {note}", file=sys.stderr)
    return EXIT_OK if pkg.coverage.all_recognised else EXIT_REVIEW


def _cmd_verify(args: argparse.Namespace) -> int:
    """Import into a live NiFi and assert nothing is invalid. Non-destructive."""
    from .validate.live import verify

    bad = verify(args.flow, args.nifi, keep=args.keep, settle_seconds=args.settle)
    if not bad:
        print("valid: NiFi accepted the flow with no flow-level validation errors")
        return EXIT_OK
    print(f"{len(bad)} component(s) invalid:", file=sys.stderr)
    for comp in bad:
        print(f"  [{comp['type']}] {comp['name']}", file=sys.stderr)
        for err in comp["errors"]:
            print(f"      - {err}", file=sys.stderr)
    return EXIT_REFUSED


def _cmd_deploy(args: argparse.Namespace) -> int:
    """Import, inject credentials from the environment, enable, start."""
    from .deploy.provision import MissingSecret, deploy

    try:
        result = deploy(args.flow, args.nifi, args.group_name, start=not args.no_start)
    except MissingSecret as exc:
        print(f"refusing to deploy: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    print(f"deployed {result['group_name']!r} ({result['group_id']})")
    print(f"  {result['secrets_injected']} sensitive propert(ies) injected from the environment")
    for name, state in sorted(result["services"].items()):
        print(f"  service {name}: {state}")
    print(f"  flow {'started' if result['started'] else 'left stopped'}")
    return EXIT_OK


def _cmd_verify_behavior(args: argparse.Namespace) -> int:
    """M5: feed a real batch through the live flow, check it against an
    independently computed expectation -- not just that the flow is valid.
    """
    from .validate import behavior, oracle

    try:
        pkg = derive(annotate(parse_file(args.package)))
    except NotADtsxPackage as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    bindings = flowdef.load_bindings(args.bindings)
    db_binding = next((b for b in bindings.values() if "identifier_case" in b), None)
    if db_binding is None:
        print("bindings file has no db binding (needs identifier_case)", file=sys.stderr)
        return EXIT_REFUSED

    fold = db_binding.get("identifier_case", "")
    lookups = oracle.lookups_from_package(pkg, fold)
    if not lookups:
        print("no lookups in this package -- nothing for the behavioural gate to check",
              file=sys.stderr)
        return EXIT_OK

    print(f"reading {len(lookups)} reference table(s) directly (not through the lookup service)",
          file=sys.stderr)
    references = {}
    for lk in lookups:
        references[lk["reference_table"]] = behavior.read_reference_table(
            args.container, args.db, args.user, lk["reference_table"],
            lk["join_column"], lk["returns"])
        print(f"  {lk['reference_table']}: {len(references[lk['reference_table']])} row(s)",
              file=sys.stderr)

    rows = behavior.make_batch()
    expected = oracle.compute(rows, lookups, references)
    print(f"batch: {len(rows)} rows -> "
          f"{sum(1 for r in expected if r['outcome']=='landed')} expected landed, "
          f"{sum(1 for r in expected if r['outcome']=='rejected')} expected rejected",
          file=sys.stderr)

    ff = next(c for c in pkg.dataflows[0].components if c.derived.get("columns"))
    d = ff.derived
    columns = [c["name"] for c in d["columns"]]
    filename = f"verify-behavior-{int(time.time())}.txt"
    landing = pathlib.Path(args.landing)
    landing.mkdir(parents=True, exist_ok=True)
    behavior.write_flat_file(rows, columns, d["column_delimiter"], d["row_delimiter"],
                              landing / filename)
    print(f"wrote {landing / filename}", file=sys.stderr)

    print(f"waiting {args.wait}s for NiFi to process it...", file=sys.stderr)
    time.sleep(args.wait)

    landed = behavior.read_landed_rows(args.container, args.db, args.user, args.fact_table)
    rejected = behavior.read_rejected_rows(pathlib.Path(args.reject_dir), filename)

    report = behavior.diff(expected, landed, rejected)
    print(f"actual: {report['actual_landed']} landed, {report['actual_rejected']} rejected")
    if report["agrees"]:
        print("agrees: every row landed or was rejected exactly where the "
              "independent check expected")
        return EXIT_OK

    print(f"{len(report['mismatches'])} mismatch(es):", file=sys.stderr)
    for m in report["mismatches"]:
        print(f"  - {m}", file=sys.stderr)
    return EXIT_REVIEW


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ssis2nifi", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    an = sub.add_parser("analyze", help="parse a .dtsx and report what it contains")
    an.add_argument("package")
    an.add_argument("--json", action="store_true", help="emit the IR instead of the report")
    an.add_argument("--no-graph", action="store_true", help="summary only, no component tree")
    an.set_defaults(func=_cmd_analyze)

    ir = sub.add_parser("ir", help="write the intermediate representation as YAML")
    ir.add_argument("package")
    ir.add_argument("-o", "--out", help="file to write (default: stdout)")
    ir.set_defaults(func=_cmd_ir)

    cv = sub.add_parser("convert", help="generate a NiFi flow definition")
    cv.add_argument("package")
    cv.add_argument("-b", "--bindings", help="bindings YAML mapping connections to real targets")
    cv.add_argument("-o", "--out", help="flow.json to write (default: out/<pkg>.flow.json)")
    cv.add_argument("--group-name", help="process group name (default: the package name)")
    cv.set_defaults(func=_cmd_convert)

    vf = sub.add_parser("verify", help="import a generated flow into NiFi and check validity")
    vf.add_argument("flow")
    vf.add_argument("--nifi", default="http://localhost:8080", help="NiFi base URL")
    vf.add_argument("--keep", action="store_true", help="leave the group on the canvas")
    vf.add_argument("--settle", type=float, default=15.0, help="seconds to wait for validation")
    vf.set_defaults(func=_cmd_verify)

    dp = sub.add_parser("deploy", help="import a flow into NiFi and start it")
    dp.add_argument("flow")
    dp.add_argument("--nifi", default="http://localhost:8080")
    dp.add_argument("--group-name")
    dp.add_argument("--no-start", action="store_true", help="import but leave stopped")
    dp.set_defaults(func=_cmd_deploy)

    vb = sub.add_parser("verify-behavior",
                         help="feed a batch through the live flow, check it against an "
                              "independently computed expectation")
    vb.add_argument("package")
    vb.add_argument("-b", "--bindings", required=True)
    vb.add_argument("--landing", required=True, help="host path the source connection manager's "
                     "directory is bind-mounted to")
    vb.add_argument("--reject-dir", required=True, help="host path the reject sink's "
                     "directory is bind-mounted to")
    vb.add_argument("--container", default="nifi-warehouse", help="Postgres container name")
    vb.add_argument("--db", required=True)
    vb.add_argument("--user", required=True)
    vb.add_argument("--fact-table", required=True, help="e.g. dbo.newfactcurrencyrate")
    vb.add_argument("--wait", type=float, default=15.0, help="seconds to let NiFi process the batch")
    vb.set_defaults(func=_cmd_verify_behavior)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
