"""CLI entry point. One image, SSIS-only jobs: the compose service that runs
this is the STREAM SOURCE for the SSIS ETL pipeline -- it continuously drops
flat order batches into the landing directory where pkg_ingest_stage picks
them up."""
from __future__ import annotations

import sys

USAGE = """usage: python -m gen <command> [args]

  stream                 continuously generate order batches into LANDING_DIR
  inject <scenario>      write one scenario batch (see `inject` with no args)
"""


def main(argv: list[str]) -> int:
    if not argv:
        print(USAGE)
        return 1

    command = argv[0]

    if command == "stream":
        from .emit import stream
        stream()

    elif command == "inject":
        from .emit import SCENARIOS, inject
        if len(argv) < 2:
            print("scenarios:")
            for name, desc in SCENARIOS.items():
                print(f"  {name:<12} {desc}")
            return 1
        inject(argv[1])

    else:
        print(f"unknown command: {command}\n")
        print(USAGE)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))