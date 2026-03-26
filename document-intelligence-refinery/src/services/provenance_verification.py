"""Provenance and audit verification: content_hash cross-check and approximate numeric match."""
from __future__ import annotations

import re
from typing import Any

from src.services.vector_store import BaseVectorStore
from src.services.numeric_parser import parse_numeric as _parse_numeric


def _normalize_hash(h: str | None) -> str:
    if not h:
        return ""
    s = (h or "").strip()[:64]
    return s.lower() if s else ""


def _content_hash_verified(
    vector_store: BaseVectorStore,
    doc_ids: list[str],
    citations: list[dict],
) -> tuple[int, int, list[dict]]:
    """Cross-check citation content_hashes against store. Returns (verified_count, total, details)."""
    total = len(citations)
    if not total or not doc_ids:
        return 0, total, []
    seen_hashes: set[str] = set()
    for doc_id in doc_ids:
        for m in vector_store.get_chunk_metadata(doc_id):
            h = _normalize_hash(m.get("content_hash"))
            if h:
                seen_hashes.add(h)
    verified = 0
    details: list[dict] = []
    for c in citations:
        ch = _normalize_hash(c.get("content_hash"))
        if not ch or ch in ("unknown", "fallback-citation", "hit-unknown"):
            details.append({"citation": c, "reason": "no_hash"})
            continue
        if ch in seen_hashes:
            verified += 1
            details.append({"citation": c, "reason": "verified"})
        else:
            details.append({"citation": c, "reason": "hash_not_in_store"})
    return verified, total, details


def _extract_numbers(text: str) -> list[float]:
    """Prefer structured parse (ranges, %, currencies); fallback to regex for free text."""
    if not text:
        return []
    parsed = _parse_numeric(str(text))
    if parsed:
        return parsed
    cleaned = re.sub(r"[,\s]", "", str(text))
    out: list[float] = []
    for m in re.finditer(r"-?\d+\.?\d*", cleaned):
        try:
            out.append(float(m.group()))
        except ValueError:
            continue
    return out


def _approx_numeric_match(
    answer: str,
    fact_values: list[str],
    relative_tolerance: float = 0.01,
) -> dict[str, Any]:
    """Check if any number in answer is within relative_tolerance of any fact value."""
    ans_nums = _extract_numbers(answer)
    fact_nums: list[float] = []
    for v in fact_values:
        fact_nums.extend(_extract_numbers(v))
    if not ans_nums or not fact_nums:
        return {"matched": False, "reason": "no_numbers"}
    matched = False
    for a in ans_nums:
        for f in fact_nums:
            if f == 0:
                if abs(a) < 1e-9:
                    matched = True
                    break
            elif abs((a - f) / f) <= relative_tolerance:
                matched = True
                break
        if matched:
            break
    return {"matched": matched, "reason": "approx_match" if matched else "no_match"}


def verify_provenance(
    vector_store: BaseVectorStore,
    doc_ids: list[str],
    citations: list[dict],
    answer: str = "",
    fact_values: list[str] | None = None,
    numeric_tolerance: float = 0.01,
) -> dict[str, Any]:
    """
    Richer verification: content_hash cross-check and optional approximate numeric match.
    Returns status in ("verified", "partial", "unverifiable") and details.
    """
    verified_count, total, details = _content_hash_verified(vector_store, doc_ids, citations)
    hash_ok = total == 0 or verified_count == total
    partial = 0 < verified_count < total if total else False

    numeric_result: dict[str, Any] = {}
    if answer and fact_values:
        numeric_result = _approx_numeric_match(answer, fact_values, relative_tolerance=numeric_tolerance)

    if total == 0:
        status = "unverifiable"
    elif hash_ok and (not numeric_result or numeric_result.get("matched") or not _extract_numbers(answer)):
        status = "verified"
    elif partial or verified_count > 0:
        status = "partial"
    else:
        status = "unverifiable"

    return {
        "status": status,
        "citations_verified": verified_count,
        "citations_total": total,
        "details": details,
        "numeric_check": numeric_result,
    }
