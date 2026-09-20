"""diff.py and render.py need no live database -- they operate purely on
capture dicts (see capture.py's docstring for that shape). These tests build
synthetic captures by hand and check the comparison logic and HTML/JSON
rendering directly, independent of Postgres/NiFi being reachable.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from report import diff, render  # noqa: E402


def _capture(engine: str, fact_count: int, reject_count: int, keys: list[str],
             reasons: dict[str, int]) -> dict:
    return {
        "engine": engine, "captured_at": "2026-01-01T00:00:00+00:00",
        "fact_table": "fact_calls", "fact_primary_key": ["call_id"],
        "scalars": {"fact_count": fact_count, "reject_count": reject_count},
        "histograms": {"reject_reasons": reasons},
        "sets": {"fact_keys": keys},
    }


def test_identical_captures_agree():
    a = _capture("SSIS", 3, 1, ["CDR1", "CDR2", "CDR3"], {"BAD_DURATION": 1})
    b = _capture("NiFi", 3, 1, ["CDR1", "CDR2", "CDR3"], {"BAD_DURATION": 1})
    result = diff.compare(a, b)
    assert result.agrees
    assert all(s.agrees for s in result.scalars)
    assert all(h.agrees for h in result.histograms)
    assert all(s.agrees for s in result.sets)


def test_a_missing_row_is_flagged_not_hidden():
    a = _capture("SSIS", 3, 1, ["CDR1", "CDR2", "CDR3"], {"BAD_DURATION": 1})
    b = _capture("NiFi", 2, 1, ["CDR1", "CDR2"], {"BAD_DURATION": 1})
    result = diff.compare(a, b)
    assert not result.agrees
    fact_count = next(s for s in result.scalars if s.key == "fact_count")
    assert not fact_count.agrees
    assert fact_count.source == 3 and fact_count.destination == 2
    fact_keys = next(s for s in result.sets if s.name == "fact_keys")
    assert fact_keys.only_source == ["CDR3"]
    assert fact_keys.only_destination == []


def test_a_reason_count_mismatch_is_flagged():
    a = _capture("SSIS", 3, 2, [], {"BAD_DURATION": 1, "UNKNOWN_TOWER": 1})
    b = _capture("NiFi", 3, 2, [], {"BAD_DURATION": 2, "UNKNOWN_TOWER": 0})
    result = diff.compare(a, b)
    assert not result.agrees
    hist = next(h for h in result.histograms if h.name == "reject_reasons")
    assert not hist.agrees


def test_html_report_renders_without_error_and_contains_verdict():
    a = _capture("SSIS", 3, 1, ["CDR1", "CDR2", "CDR3"], {"BAD_DURATION": 1})
    b = _capture("NiFi", 3, 1, ["CDR1", "CDR2", "CDR3"], {"BAD_DURATION": 1})
    result = diff.compare(a, b)
    nifi_state = {"found": True, "group_name": "telecom_cdr", "group_id": "abc",
                  "state": "RUNNING", "running": 5, "stopped": 0, "invalid": 0,
                  "processors": [], "bulletins": []}
    html = render.render_html("telecom_cdr", result, nifi_state, [])
    assert "AGREES" in html
    assert "telecom_cdr" in html
    assert "<html>" in html


def test_json_export_round_trips_through_json_module():
    import json
    a = _capture("SSIS", 1, 0, ["CDR1"], {})
    b = _capture("NiFi", 1, 0, ["CDR1"], {})
    result = diff.compare(a, b)
    text = render.to_json("telecom_cdr", result, {"found": False, "group_name": "x"}, [])
    parsed = json.loads(text)
    assert parsed["agrees"] is True
    assert parsed["job"] == "telecom_cdr"
