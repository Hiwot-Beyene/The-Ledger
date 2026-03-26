"""
Extract key-value numerical facts from LDU chunks (e.g. row-semantic table lines)
and persist them into the fact table for structured_query.
"""
from __future__ import annotations

import re
from pathlib import Path

from src.services.fact_table import (
    delete_facts_by_doc_id,
    init_fact_table,
    upsert_fact,
)
from src.services.numeric_parser import first_numeric as _first_numeric_parsed

LABEL_VALUE_LINE = re.compile(r"^([^:]+):\s*(.+)$")


def _label_to_fact_key(label: str) -> str:
    key = (label or "").strip().lower()
    key = re.sub(r"[^a-z0-9]+", "_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key or "unknown"


def _first_numeric_value(value_str: str) -> str | None:
    value_str = (value_str or "").strip()
    parts = value_str.split("|")
    first_part = (parts[0] or "").strip() if parts else ""
    return _first_numeric_parsed(first_part)


def extract_facts_from_chunks(
    db_path: str | Path,
    doc_id: str,
    chunks: list[dict],
) -> int:
    """
    Parse chunks for label: value lines (e.g. from table LDUs), normalize to fact_key/fact_value,
    and insert into the fact table. Returns the number of facts inserted.
    """
    init_fact_table(db_path)
    delete_facts_by_doc_id(db_path, doc_id)
    count = 0
    for chunk in chunks or []:
        text = (chunk.get("text") or "").strip()
        if not text:
            continue
        page_refs = chunk.get("page_refs") or [1]
        page_number = int(page_refs[0]) if page_refs else 1
        content_hash = (chunk.get("content_hash") or "")[:64] or "unknown"
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("Columns:") or line.startswith("|"):
                continue
            mo = LABEL_VALUE_LINE.match(line)
            if not mo:
                continue
            label, value_str = mo.group(1), mo.group(2)
            fact_key = _label_to_fact_key(label)
            if not fact_key or fact_key == "unknown":
                continue
            fact_value = _first_numeric_value(value_str)
            if fact_value is None:
                continue
            upsert_fact(
                db_path=db_path,
                doc_id=doc_id,
                fact_key=fact_key,
                fact_value=fact_value,
                page_number=page_number,
                content_hash=content_hash,
            )
            count += 1
    return count
