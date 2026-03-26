from pathlib import Path

import pytest

from src.services.fact_table import init_fact_table, structured_query, upsert_fact


def test_init_creates_indexes(tmp_path: Path):
    db = tmp_path / "facts.db"
    init_fact_table(db)
    import sqlite3
    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_facts%'"
    ).fetchall()
    conn.close()
    names = {r[0] for r in rows}
    assert "idx_facts_fact_key_doc_id" in names
    assert "idx_facts_doc_id" in names


def test_structured_query_uses_index_for_lookup(tmp_path: Path):
    db = tmp_path / "facts.db"
    init_fact_table(db)
    upsert_fact(db, "d1", "revenue", "1000", 1, "h1")
    upsert_fact(db, "d2", "revenue", "2000", 1, "h2")
    rows = structured_query(db, ["d1", "d2"], "revenue")
    assert len(rows) == 2
    assert {r["doc_id"] for r in rows} == {"d1", "d2"}
