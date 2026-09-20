"""Compares two captures (see capture.py) -- structurally the same
scalars/histograms/sets comparison as the reference repo's
destination/spec/compare/diff.py, restructured to return data (for
render.py to turn into HTML/JSON) instead of printing to a terminal.

A difference here is not automatically a bug: it is a question about
whether the two engines saw the same input and agree on the rules. This
module only says WHERE they differ.
"""
from __future__ import annotations

import dataclasses


@dataclasses.dataclass
class ScalarDiff:
    key: str
    source: int | None
    destination: int | None

    @property
    def agrees(self) -> bool:
        return self.source is not None and self.source == self.destination


@dataclasses.dataclass
class HistogramDiff:
    name: str
    rows: list[tuple[str, int | None, int | None]]  # (bucket, source, destination)

    @property
    def agrees(self) -> bool:
        return all(s == d for _, s, d in self.rows)


@dataclasses.dataclass
class SetDiff:
    name: str
    only_source: list[str]
    only_destination: list[str]
    in_both: int

    @property
    def agrees(self) -> bool:
        return not self.only_source and not self.only_destination


@dataclasses.dataclass
class ComparisonResult:
    source: dict
    destination: dict
    scalars: list[ScalarDiff]
    histograms: list[HistogramDiff]
    sets: list[SetDiff]

    @property
    def agrees(self) -> bool:
        return (all(s.agrees for s in self.scalars)
                and all(h.agrees for h in self.histograms)
                and all(s.agrees for s in self.sets))


def compare_scalars(a: dict, b: dict) -> list[ScalarDiff]:
    keys = sorted(set(a) | set(b))
    return [ScalarDiff(key=k, source=a.get(k), destination=b.get(k)) for k in keys]


def compare_histograms(a: dict, b: dict) -> list[HistogramDiff]:
    out = []
    for name in sorted(set(a) | set(b)):
        ha, hb = a.get(name, {}), b.get(name, {})
        buckets = sorted(set(ha) | set(hb))
        out.append(HistogramDiff(
            name=name,
            rows=[(bucket, ha.get(bucket), hb.get(bucket)) for bucket in buckets],
        ))
    return out


def compare_sets(a: dict, b: dict) -> list[SetDiff]:
    out = []
    for name in sorted(set(a) | set(b)):
        sa, sb = set(a.get(name, [])), set(b.get(name, []))
        out.append(SetDiff(
            name=name,
            only_source=sorted(sa - sb),
            only_destination=sorted(sb - sa),
            in_both=len(sa & sb),
        ))
    return out


def compare(source_capture: dict, destination_capture: dict) -> ComparisonResult:
    return ComparisonResult(
        source=source_capture,
        destination=destination_capture,
        scalars=compare_scalars(source_capture["scalars"], destination_capture["scalars"]),
        histograms=compare_histograms(source_capture["histograms"], destination_capture["histograms"]),
        sets=compare_sets(source_capture["sets"], destination_capture["sets"]),
    )
