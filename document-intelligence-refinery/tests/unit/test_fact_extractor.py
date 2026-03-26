from __future__ import annotations

from pathlib import Path

import pytest

from src.services.fact_extractor import extract_facts_from_chunks
from src.services.fact_table import structured_query


def test_extract_facts_from_chunks_parses_table_lines(tmp_path: Path):
    db = tmp_path / "facts.db"
    chunks = [
        {
            "text": "Columns: Notes | 2022 | 2021\nRevenue: 59,448,361 | 55,499,196\nTotal comprehensive income for the year: 9,063,685 | 5,651,046",
            "page_refs": [1],
            "content_hash": "abc123",
        },
    ]
    n = extract_facts_from_chunks(db, "doc1", chunks)
    assert n >= 2
    rev = structured_query(db, ["doc1"], "revenue")
    assert len(rev) == 1
    assert rev[0]["fact_value"] == "59448361"
    tci = structured_query(db, ["doc1"], "total_comprehensive_income_for_the_year")
    assert len(tci) == 1
    assert tci[0]["fact_value"] == "9063685"


def test_extract_facts_replaces_previous_doc_facts(tmp_path: Path):
    db = tmp_path / "facts.db"
    chunks1 = [{"text": "Profit: 100", "page_refs": [1], "content_hash": "h1"}]
    extract_facts_from_chunks(db, "doc1", chunks1)
    chunks2 = [{"text": "Profit: 200", "page_refs": [1], "content_hash": "h2"}]
    extract_facts_from_chunks(db, "doc1", chunks2)
    rows = structured_query(db, ["doc1"], "profit")
    assert len(rows) == 1
    assert rows[0]["fact_value"] == "200"
