from __future__ import annotations

import sqlite3
from pathlib import Path


def init_fact_table(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT NOT NULL,
            fact_key TEXT NOT NULL,
            fact_value TEXT NOT NULL,
            page_number INTEGER NOT NULL,
            content_hash TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_facts_fact_key_doc_id ON facts(fact_key, doc_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_facts_doc_id ON facts(doc_id)")
    conn.commit()
    conn.close()


def upsert_fact(db_path: str | Path, doc_id: str, fact_key: str, fact_value: str, page_number: int, content_hash: str) -> None:
    init_fact_table(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO facts (doc_id, fact_key, fact_value, page_number, content_hash) VALUES (?, ?, ?, ?, ?)",
        (doc_id, fact_key, fact_value, page_number, content_hash),
    )
    conn.commit()
    conn.close()


def delete_facts_by_doc_id(db_path: str | Path, doc_id: str) -> None:
    init_fact_table(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("DELETE FROM facts WHERE doc_id = ?", (doc_id,))
    conn.commit()
    conn.close()


def structured_query(db_path: str | Path, doc_ids: list[str], key: str) -> list[dict]:
    init_fact_table(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    if doc_ids:
        placeholders = ",".join("?" for _ in doc_ids)
        rows = conn.execute(
            f"SELECT doc_id, fact_key, fact_value, page_number, content_hash FROM facts WHERE fact_key = ? AND doc_id IN ({placeholders})",
            [key, *doc_ids],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT doc_id, fact_key, fact_value, page_number, content_hash FROM facts WHERE fact_key = ?",
            (key,),
        ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def structured_query_multi(
    db_path: str | Path, doc_ids: list[str], keys: list[str]
) -> list[dict]:
    """Query facts for multiple keys; returns concatenated results."""
    out: list[dict] = []
    seen: set[tuple[str, str, int]] = set()
    for key in keys:
        for row in structured_query(db_path, doc_ids, key):
            dedup = (row.get("doc_id"), row.get("fact_key"), row.get("page_number"))
            if dedup not in seen:
                seen.add(dedup)
                out.append(row)
    return out
