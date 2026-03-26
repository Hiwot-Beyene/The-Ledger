import pytest

from src.services.numeric_parser import parse_numeric, first_numeric


def test_plain_and_comma_sep():
    assert parse_numeric("24,782,408") == [24782408.0]
    assert parse_numeric("9,063,685") == [9063685.0]
    assert first_numeric("1,234.56") == "1234.56"


def test_ranges():
    assert parse_numeric("100 - 200") == [100.0, 200.0]
    assert parse_numeric("100–200") == [100.0, 200.0]
    assert first_numeric("50 - 60") == "50"


def test_percentages():
    assert parse_numeric("25%") == [25.0]
    assert parse_numeric("12.5%") == [12.5]
    assert parse_numeric("12,5%") == [12.5]
    assert first_numeric("33%") == "33"


def test_currencies():
    assert parse_numeric("$1,234.56") == [1234.56]
    assert parse_numeric("€1.000,50") == [1000.5]
    assert parse_numeric("1,234 USD") == [1234.0]
    assert first_numeric("$ 99") == "99"


def test_negative_parens():
    assert parse_numeric("(1,234)") == [-1234.0]
    assert first_numeric("(500)") == "-500"


def test_first_numeric_none():
    assert first_numeric("no numbers") is None
    assert first_numeric("") is None
