from __future__ import annotations

import hashlib
from datetime import datetime, timezone


def create_langsmith_trace_id(query_id: str, tool_sequence: list[str]) -> str:
    payload = f"{query_id}|{'->'.join(tool_sequence)}|{datetime.now(timezone.utc).isoformat()}"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
    return f"ls-{digest}"


def required_trace_metadata(
    query_id: str,
    doc_id: str,
    provider: str,
    model: str,
    tool_sequence: list[str],
    citation_count: int,
) -> dict[str, str | int]:
    return {
        "query_id": query_id,
        "doc_id": doc_id,
        "provider": provider,
        "model": model,
        "tool_sequence": "->".join(tool_sequence),
        "citation_count": citation_count,
    }
