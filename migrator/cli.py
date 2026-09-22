#!/usr/bin/env python3
"""migrator -- the dynamic SSIS -> NiFi migration tool.

Every subcommand takes a JOB: either a path to a job.yml, or a bare job name
resolved as jobs/<name>/job.yml relative to the current directory. A job.yml
is the only place any table/column/connection-target fact lives (see
jobconfig.py's docstring) -- this file and everything it calls into
(ssis2nifi/, bindings.py, sidecar.py) contain no domain-specific literal.

Exit codes match the vendored converter's own contract (see
ssis2nifi/__main__.py in the reference repo): 0 = converted cleanly,
3 = parsed but needs human review, 4 = refused outright.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import jobconfig
import bindings as bindings_mod
import sidecar
from ssis2nifi.catalog.derive import derive
from ssis2nifi.catalog.support import annotate
from ssis2nifi.dtsx.parse import NotADtsxPackage, parse_file
from ssis2nifi.emit import flowdef
from ssis2nifi.report import graph

EXIT_OK, EXIT_REVIEW, EXIT_REFUSED = 0, 3, 4


def _cmd_analyze(args: argparse.Namespace) -> int:
    job = jobconfig.resolve(args.job)
    try:
        pkg = derive(annotate(parse_file(str(job.package_path))))
    except NotADtsxPackage as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    if args.json:
        print(json.dumps(pkg.to_dict(), indent=2, default=str))
    else:
        print(graph.render(pkg, show_graph=not args.no_graph))
    return EXIT_OK if pkg.coverage.all_recognised else EXIT_REVIEW


def _convert(job: jobconfig.JobConfig, out_dir: pathlib.Path,
             regenerate_bindings: bool) -> tuple[int, pathlib.Path | None]:
    try:
        pkg = derive(annotate(parse_file(str(job.package_path))))
    except NotADtsxPackage as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED, None

    print(f"coverage: {pkg.coverage.recognised}/{pkg.coverage.total_components} "
          f"components recognised", file=sys.stderr)
    if not pkg.coverage.all_recognised:
        print("needs human review before this package can be converted", file=sys.stderr)
        return EXIT_REVIEW, None

    bindings_path = bindings_mod.resolve(job, force_regenerate=regenerate_bindings)
    bindings = flowdef.load_bindings(str(bindings_path))
    try:
        flow, secrets, notes = flowdef.build(pkg, bindings, group_name=job.name)
    except flowdef.EmitError as exc:
        print(f"cannot generate a flow: {exc}", file=sys.stderr)
        return EXIT_REFUSED, None

    out_dir.mkdir(parents=True, exist_ok=True)
    flow_path = out_dir / f"{job.name}.flow.json"
    flow_path.write_text(json.dumps(flow, indent=2))
    mapping_path = sidecar.write(pkg, flow_path)

    fc = flow["flowContents"]
    print(f"wrote {flow_path}", file=sys.stderr)
    print(f"  {len(fc['processors'])} processors, {len(fc['connections'])} connections, "
          f"{len(fc['controllerServices'])} controller services", file=sys.stderr)
    print(f"  wrote {mapping_path}", file=sys.stderr)

    if secrets:
        side = flow_path.with_suffix("").with_suffix(".secrets.json")
        side.write_text(json.dumps(secrets, indent=2))
        print(f"  {len(secrets)} sensitive propert(ies) left null; see {side.name}",
              file=sys.stderr)

    for note in notes:
        print(f"  note: {note}", file=sys.stderr)
    return EXIT_OK, flow_path


def _cmd_convert(args: argparse.Namespace) -> int:
    job = jobconfig.resolve(args.job)
    code, _ = _convert(job, pathlib.Path(args.out or "out"), args.regenerate_bindings)
    return code


def _cmd_deploy(args: argparse.Namespace) -> int:
    from ssis2nifi.deploy.provision import MissingSecret, deploy

    job = jobconfig.resolve(args.job)
    code, flow_path = _convert(job, pathlib.Path(args.out or "out"), args.regenerate_bindings)
    if code != EXIT_OK:
        return code

    try:
        result = deploy(str(flow_path), job.nifi_url, job.name, start=not args.no_start)
    except MissingSecret as exc:
        print(f"refusing to deploy: {exc}", file=sys.stderr)
        return EXIT_REFUSED

    print(f"deployed {result['group_name']!r} ({result['group_id']}) to {job.nifi_url}")
    print(f"  {result['secrets_injected']} sensitive propert(ies) injected from the environment")
    for name, state in sorted(result["services"].items()):
        print(f"  service {name}: {state}")
    print(f"  flow {'started' if result['started'] else 'left stopped'}")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="migrator", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    an = sub.add_parser("analyze", help="parse a job's .dtsx and report what it contains")
    an.add_argument("job", help="job.yml path, a job directory, or a bare name under jobs/")
    an.add_argument("--json", action="store_true", help="emit the IR instead of the report")
    an.add_argument("--no-graph", action="store_true", help="summary only, no component tree")
    an.set_defaults(func=_cmd_analyze)

    cv = sub.add_parser("convert", help="generate a NiFi flow definition, offline")
    cv.add_argument("job")
    cv.add_argument("-o", "--out", help="directory to write flow.json into (default: out/)")
    cv.add_argument("--regenerate-bindings", action="store_true",
                     help="overwrite jobs/<name>/bindings.generated.yml even if present")
    cv.set_defaults(func=_cmd_convert)

    dp = sub.add_parser("deploy", help="convert, then import into NiFi and start it")
    dp.add_argument("job")
    dp.add_argument("-o", "--out", help="directory to write flow.json into (default: out/)")
    dp.add_argument("--regenerate-bindings", action="store_true")
    dp.add_argument("--no-start", action="store_true", help="import but leave stopped")
    dp.set_defaults(func=_cmd_deploy)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
