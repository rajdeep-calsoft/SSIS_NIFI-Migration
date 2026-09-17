"""CLI entry point. One image, several jobs -- the compose services differ
only by the argument they are launched with."""
from __future__ import annotations

import sys

USAGE = """usage: python -m gen <command> [args]

  stream                 continuously generate order batches into LANDING_DIR
  inject <scenario>      write one scenario batch (see `inject` with no args)
  monitor                poll the NiFi REST API into Postgres
  provision              import the flow definition into NiFi and start it
  build-flow             author the flow from scratch via the REST API (dev)
  export-flow            dump the live flow back to nifi/flow/*.json (dev)
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

    elif command == "monitor":
        from .monitor import run
        run()

    elif command == "provision":
        from .provision import run
        return run()

    elif command == "build-flow":
        from .build_flow import run
        return run()

    elif command == "export-flow":
        from .export_flow import run
        return run()

    else:
        print(f"unknown command: {command}\n")
        print(USAGE)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
