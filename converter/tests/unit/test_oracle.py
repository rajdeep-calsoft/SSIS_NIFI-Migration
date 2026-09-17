"""M5: the independent oracle, tested entirely offline.

No NiFi, no Postgres, no Docker -- `oracle.py` is a pure function over
in-memory reference tables, by design (see its module docstring). The live
half (`behavior.py`'s docker/psql calls) is exercised by
`make verify-behavior`, not here.
"""

from __future__ import annotations

import pathlib
import re

from ssis2nifi.validate import behavior, oracle

from .conftest import CORPUS, l1  # noqa: F401


def test_lookups_from_package_folds_identifiers_like_the_binding_does(l1):
    lookups = oracle.lookups_from_package(l1, identifier_case="lower")
    by_table = {lk["reference_table"]: lk for lk in lookups}

    assert set(by_table) == {"dbo.dimcurrency", "dbo.dimdate"}
    currency = by_table["dbo.dimcurrency"]
    assert currency["node"] == "Lookup Currency Key"
    assert currency["input_column"] == "CurrencyID"
    assert currency["join_column"] == "currencyalternatekey"
    assert currency["returns"] == ["currencykey"]
    assert currency["no_match_fails"] is True


def test_lookups_from_package_without_folding_keeps_raw_casing(l1):
    lookups = oracle.lookups_from_package(l1, identifier_case="")
    by_table = {lk["reference_table"]: lk for lk in lookups}
    assert "dbo.DimCurrency" in by_table
    assert by_table["dbo.DimCurrency"]["join_column"] == "CurrencyAlternateKey"


# --- compute(): the matching logic, independent of both NiFi and the emitter

_LOOKUPS = [
    {"node": "Lookup Currency Key", "input_column": "CurrencyID",
     "reference_table": "dbo.dimcurrency", "join_column": "currencyalternatekey",
     "returns": ["currencykey"], "no_match_fails": True},
    {"node": "Lookup Date Key", "input_column": "CurrencyDate",
     "reference_table": "dbo.dimdate", "join_column": "fulldatealternatekey",
     "returns": ["datekey"], "no_match_fails": True},
]

_REFERENCES = {
    "dbo.dimcurrency": {"GBP": {"currencykey": "36"}, "USD": {"currencykey": "98"}},
    "dbo.dimdate": {"2014-01-01": {"datekey": "20140101"}},
}


def test_a_row_matching_both_lookups_lands_with_both_surrogate_keys():
    rows = [{"CurrencyID": "GBP", "CurrencyDate": "2014-01-01"}]
    [out] = oracle.compute(rows, _LOOKUPS, _REFERENCES)
    assert out["outcome"] == "landed"
    assert out["rejected_at"] is None
    assert out["currencykey"] == "36"
    assert out["datekey"] == "20140101"


def test_a_row_missing_the_first_lookup_is_rejected_there_and_never_reaches_the_second():
    rows = [{"CurrencyID": "ZZZ", "CurrencyDate": "2014-01-01"}]
    [out] = oracle.compute(rows, _LOOKUPS, _REFERENCES)
    assert out["outcome"] == "rejected"
    assert out["rejected_at"] == "Lookup Currency Key"
    assert "datekey" not in out          # never got there


def test_a_row_matching_the_first_but_missing_the_second_is_rejected_at_the_second():
    rows = [{"CurrencyID": "GBP", "CurrencyDate": "2099-06-15"}]
    [out] = oracle.compute(rows, _LOOKUPS, _REFERENCES)
    assert out["outcome"] == "rejected"
    assert out["rejected_at"] == "Lookup Date Key"
    assert out["currencykey"] == "36"    # the first lookup's result is still attached


def test_a_miss_on_a_lookup_that_does_not_fail_passes_through_unenriched():
    lax = [{**_LOOKUPS[0], "no_match_fails": False}]
    rows = [{"CurrencyID": "ZZZ", "CurrencyDate": "2014-01-01"}]
    [out] = oracle.compute(rows, lax, _REFERENCES)
    assert out["outcome"] == "landed"
    assert "currencykey" not in out


# --- behavior.make_batch() / diff(): pure, no I/O -------------------------

def test_make_batch_gives_every_row_a_distinct_tracer():
    rows = behavior.make_batch()
    tracers = [r["AverageRate"] for r in rows]
    assert len(tracers) == len(set(tracers)), "AverageRate must be unique per row to trace it back"


def test_make_batch_covers_every_cell_of_the_disposition_matrix():
    """Every branch DIAGRAM.md picture 5 describes must actually be exercised."""
    lookups = _LOOKUPS
    references = {
        "dbo.dimcurrency": {c: {"currencykey": str(i)}
                             for i, c in enumerate(["GBP", "EUR", "USD"])},
        "dbo.dimdate": {d: {"datekey": "1"} for d in ["2014-01-01", "2014-01-02", "2013-12-31"]},
    }
    expected = oracle.compute(behavior.make_batch(), lookups, references)
    outcomes = {(r["outcome"], r["rejected_at"]) for r in expected}
    assert ("landed", None) in outcomes
    assert ("rejected", "Lookup Currency Key") in outcomes
    assert ("rejected", "Lookup Date Key") in outcomes


def test_diff_reports_no_mismatches_when_reality_matches_the_oracle():
    expected = [{"AverageRate": 1.0, "outcome": "landed", "rejected_at": None},
                {"AverageRate": 2.0, "outcome": "rejected", "rejected_at": "Lookup Currency Key"}]
    landed = {"1.000": {"averagerate": "1.0"}}
    rejected = {"2.000": {"AverageRate": 2.0}}
    report = behavior.diff(expected, landed, rejected)
    assert report["agrees"]
    assert report["mismatches"] == []


def test_diff_catches_a_row_that_should_have_landed_but_was_rejected():
    expected = [{"AverageRate": 1.0, "outcome": "landed", "rejected_at": None}]
    report = behavior.diff(expected, landed={}, rejected={"1.000": {}})
    assert not report["agrees"]
    assert "expected LANDED" in report["mismatches"][0]


def test_diff_catches_a_row_that_fired_both_paths():
    """The reject-sink filename collision this caught: see behavior.py."""
    expected = [{"AverageRate": 1.0, "outcome": "landed", "rejected_at": None}]
    report = behavior.diff(expected, landed={"1.000": {}}, rejected={"1.000": {}})
    assert not report["agrees"]
    assert "both fired" in report["mismatches"][0]


def test_tracer_matches_a_whole_number_however_postgres_prints_it():
    """Postgres prints a whole-number `real` back as "9", not "9.0" --
    str(9.0) in Python is "9.0". A naive string comparison would call a
    correctly landed row "missing entirely". Found running the live check
    against corpus/packages/L1.dtsx: row AverageRate=9.0 landed correctly but
    was reported missing until this normalisation was added.
    """
    expected = [{"AverageRate": 9.0, "outcome": "landed", "rejected_at": None}]
    landed = {behavior._tracer("9"): {"averagerate": "9"}}
    report = behavior.diff(expected, landed, rejected={})
    assert report["agrees"]


def test_write_flat_file_round_trips_through_the_same_delimiters(tmp_path):
    rows = [{"AverageRate": 1.0, "CurrencyID": "GBP", "CurrencyDate": "2014-01-01",
             "EndOfDayRate": 1.1}]
    out = tmp_path / "batch.txt"
    behavior.write_flat_file(rows, ["AverageRate", "CurrencyID", "CurrencyDate", "EndOfDayRate"],
                              "\t", "\r\n", out)
    text = out.read_bytes().decode("utf-8")
    assert text == "1.0\tGBP\t2014-01-01\t1.1\r\n"


# --- the boundary: the oracle must not import the emitter -----------------

def test_the_oracle_does_not_import_emit():
    """If oracle.py imported emit/flowdef.py, agreement would prove nothing --
    it would just mean the oracle copied the generator's own logic."""
    src = (pathlib.Path(__file__).resolve().parents[2] / "ssis2nifi" / "validate" / "oracle.py")
    code = re.sub(r'(?s)""".*?"""', "", src.read_text())
    assert "emit" not in code
    assert "flowdef" not in code
