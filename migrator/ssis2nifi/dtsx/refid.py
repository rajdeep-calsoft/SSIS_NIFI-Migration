"""Parsing SSIS refIds.

A refId is SSIS's path-like identity for everything in a package:

    Package\\Extract Sample Currency Data\\Lookup Currency Key.Outputs[Lookup Match Output].Columns[CurrencyKey]
    ^-- package    ^-- data flow task      ^-- component        ^-- port kind + name      ^-- column

Two reasons this gets its own module rather than a regex at each call site:

1. It is the join key for the whole tool.  `<path startId=... endId=...>` wires
   components together by refId, and `lineageId` points at a *column* refId.
   Resolving those is how the data flow graph is recovered at all.

2. It is the provenance key.  Every generated NiFi processor derives its uuid5
   from the refId of the SSIS component that produced it, so a processor on a
   running canvas can be traced back to a line of XML.  That only works if the
   parse is exact and stable.

Backslash is the separator, and component names may legally contain spaces,
dots and brackets -- so the port suffix is split off from the RIGHT, once, and
only when it matches the bracketed form.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# `.Outputs[Name]` / `.Inputs[Name]` / `.Columns[Name]` / `.Connections[Name]`,
# anchored at the end and peeled off one suffix at a time.
#
# The name is `[^]]*` rather than `.*?` on purpose. A non-greedy `.*?` still has
# to reach the anchored `]$`, so on
#   ...Outputs[Lookup Match Output].Columns[CurrencyKey]
# it captures `Lookup Match Output].Columns[CurrencyKey` as one name and loses
# the Columns segment entirely. Excluding `]` keeps each suffix to its own
# brackets. Port names may contain spaces and dots, but never a `]`.
_PORT = re.compile(r"\.(Outputs|Inputs|Columns|Connections|Paths)\[([^\]]*)\]$")


@dataclass(frozen=True)
class RefId:
    """One parsed refId.

    `path` is the backslash-separated owner chain with the port suffix removed.
    `ports` is the chain of (kind, name) suffixes, outermost last, so
    `...Outputs[X].Columns[Y]` gives [("Outputs", "X"), ("Columns", "Y")].
    """

    raw: str
    path: tuple[str, ...]
    ports: tuple[tuple[str, str], ...]

    @property
    def owner(self) -> str:
        """The refId of the thing that owns this port, i.e. minus all suffixes."""
        return "\\".join(self.path)

    @property
    def leaf(self) -> str:
        """Last path segment -- usually the component name."""
        return self.path[-1] if self.path else ""

    def port(self, kind: str) -> str | None:
        """The name of the first port of `kind`, or None."""
        for k, name in self.ports:
            if k == kind:
                return name
        return None

    def __str__(self) -> str:  # keep the original spelling for reports
        return self.raw


def parse(raw: str) -> RefId:
    """Parse a refId. Never raises -- an unparseable refId still round-trips.

    A refId we do not understand is data we must not silently lose, so the raw
    string is always preserved and the caller can decide what to do.
    """
    rest = raw
    ports: list[tuple[str, str]] = []
    while True:
        m = _PORT.search(rest)
        if not m:
            break
        ports.append((m.group(1), m.group(2)))
        rest = rest[: m.start()]
    ports.reverse()
    path = tuple(p for p in rest.split("\\") if p)
    return RefId(raw=raw, path=path, ports=tuple(ports))


def component_of(raw: str) -> str:
    """The owning component's refId for any port/column refId.

    This is the function the graph builder actually calls: given a path's
    `startId` (which names an *output*) or a column's `lineageId` (which names
    a *column of an output*), return the component that owns it.
    """
    return parse(raw).owner
