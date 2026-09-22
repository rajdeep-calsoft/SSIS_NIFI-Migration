"""catalog/expr.py: the SSIS expression -> Calcite SQL translator.

Bounded subset by design (see expr.py's module docstring) -- these tests
assert both what it accepts and what it refuses, since refusing correctly is
as much the point as translating correctly.
"""
from __future__ import annotations

import pytest

from ssis2nifi.catalog import expr


@pytest.mark.parametrize("ssis, sql", [
    ("qty >= 1", "(qty >= 1)"),
    ("qty == 1", "(qty = 1)"),
    ("qty != 1", "(qty <> 1)"),
    ("qty >= 1 && qty <= 999", "((qty >= 1) AND (qty <= 999))"),
    ("a || b", "(a OR b)"),
    ("!(a == 1)", "(NOT (a = 1))"),
    ("UPPER(currency)", "UPPER(currency)"),
    ('UPPER(currency) == "INR"', "(UPPER(currency) = 'INR')"),
    ("ISNULL(sku)", "(sku IS NULL)"),
    ("1 + 2", "(1 + 2)"),
    ("1.5 * qty", "(1.5 * qty)"),
])
def test_translates_supported_subset(ssis, sql):
    assert expr.translate(ssis) == sql


def test_plus_is_arithmetic_between_numbers():
    assert expr.translate("qty + 1") == "(qty + 1)"


def test_plus_is_concat_with_a_string_literal():
    assert expr.translate('a + "x"') == "(a || 'x')"


def test_plus_is_concat_with_a_string_cast():
    # order_id + "#" + (DT_WSTR,10) line_no -- the exact shape
    # pkg_orders_etl.dtsx uses to build order_line_id.
    got = expr.translate('order_id + "#" + (DT_WSTR,10) line_no')
    assert got == "((order_id || '#') || CAST(line_no AS VARCHAR))"


def test_ternary_becomes_case():
    assert expr.translate('a == 1 ? "yes" : "no"') == \
        "(CASE WHEN (a = 1) THEN 'yes' ELSE 'no' END)"


def test_precedence_matches_expectation():
    # && binds tighter than ||, matching C-family precedence SSIS follows.
    assert expr.translate("a || b && c") == "(a OR (b AND c))"


def test_string_literal_escaping_round_trips():
    assert expr.translate(r'a == "it\"s"') == "(a = 'it\"s')"
    assert expr.translate("a == \"o'clock\"") == "(a = 'o''clock')"


@pytest.mark.parametrize("bad", [
    'DATEADD("d", 1, x)',      # function outside KNOWN_FUNCTIONS
    "a ===  b",                 # not a real operator
    "(DT_CY) a",                # cast type outside KNOWN_CASTS
    "a &&",                     # incomplete expression
    "a == 1 b",                 # trailing garbage
])
def test_refuses_unsupported_constructs(bad):
    with pytest.raises(expr.ExprUnsupported):
        expr.translate(bad)
