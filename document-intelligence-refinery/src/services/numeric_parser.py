"""
Parse numbers from text: ranges (100–200), percentages (25%), currencies ($1,234.56; €1.000,50).
Used by fact extraction and provenance verification.
"""
from __future__ import annotations

import re
from typing import Optional

RANGE_SEP = re.compile(r"\s*[-–—]\s*")
PERCENT = re.compile(r"(-?\d+(?:[.,]\d+)*)\s*%")
CURRENCY_PREFIX = re.compile(r"^[$€£¥]\s*")
CURRENCY_SUFFIX = re.compile(r"\s*(?:USD|EUR|GBP|JPY|CHF)$", re.I)
COMMA_DECIMAL = re.compile(r"^(-?\d{1,3}(?:\.\d{3})*(?:,\d+)?)$")
US_THOUSANDS = re.compile(r"^-?\d{1,3}(?:,\d{3})*(?:\.\d+)?$")
NEG_PARENS = re.compile(r"\(([^)]+)\)")


def _normalize_digit_string(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("\u00a0", " ")
    if not s:
        return ""
    if US_THOUSANDS.match(s):
        return s.replace(",", "")
    if re.match(r"^-?\d+,\d{1,2}$", s):
        return s.replace(",", ".")
    if COMMA_DECIMAL.match(s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    return s


def _to_float(s: str) -> Optional[float]:
    s = _normalize_digit_string(s)
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_numeric(value_str: str) -> list[float]:
    """
    Extract numeric values from a string. Handles:
    - Ranges: "100 - 200", "100–200" -> [100.0, 200.0]
    - Percentages: "25%", "12.5%" -> [25.0], [12.5]
    - Currencies: "$1,234.56", "€1.000,50", "1,234 USD" -> [1234.56], [1000.50], [1234.0]
    - Parentheses for negative: "(1,234)" -> [-1234.0]
    - Plain: "1,234.56"
    """
    if not value_str or not isinstance(value_str, str):
        return []
    raw = value_str.strip()
    if not raw:
        return []
    raw = CURRENCY_PREFIX.sub("", raw)
    raw = CURRENCY_SUFFIX.sub("", raw).strip()
    out: list[float] = []

    segments = RANGE_SEP.split(raw)
    for segment in segments:
        segment = segment.strip()
        if NEG_PARENS.search(segment):
            segment = NEG_PARENS.sub(r"-\1", segment)
        pct_matches = list(PERCENT.finditer(segment))
        if pct_matches:
            for pct in pct_matches:
                v = _to_float(pct.group(1))
                if v is not None:
                    out.append(v)
        else:
            remainder = PERCENT.sub("", segment).strip()
            remainder = NEG_PARENS.sub(r"-\1", remainder).strip()
            v = _to_float(remainder)
            if v is not None:
                out.append(v)
    if not out and raw:
        v = _to_float(NEG_PARENS.sub(r"-\1", raw))
        if v is not None:
            out.append(v)
    return out


def first_numeric(value_str: str) -> Optional[str]:
    """Return first parsed number as string (for fact_value storage), or None."""
    parsed = parse_numeric(value_str)
    if not parsed:
        return None
    v = parsed[0]
    if v == int(v):
        return str(int(v))
    return str(v)
