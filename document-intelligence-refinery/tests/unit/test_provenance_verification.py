import pytest

from src.services.provenance_verification import (
    verify_provenance,
    _normalize_hash,
    _extract_numbers,
    _approx_numeric_match,
)
from src.services.vector_store import InMemoryVectorStore


def test_normalize_hash():
    assert _normalize_hash("") == ""
    assert _normalize_hash(None) == ""
    assert _normalize_hash("abc123") == "abc123"
    assert _normalize_hash("  ABC  ")[:3] == "abc"


def test_extract_numbers():
    assert _extract_numbers("24,782,408") == [24782408.0]
    assert _extract_numbers("9,063,685") == [9063685.0]
    assert _extract_numbers("Revenue was 100.5 and 200") == [100.5, 200.0]
    assert _extract_numbers("no numbers") == []


def test_approx_numeric_match():
    r = _approx_numeric_match("Total 24782408", ["24782408"], relative_tolerance=0.01)
    assert r["matched"] is True
    r = _approx_numeric_match("Total 24782408", ["24000000"], relative_tolerance=0.05)
    assert r["matched"] is True
    r = _approx_numeric_match("Total 100", ["200"], relative_tolerance=0.01)
    assert r["matched"] is False


def test_verify_provenance_no_citations():
    store = InMemoryVectorStore()
    r = verify_provenance(store, ["d1"], [])
    assert r["status"] == "unverifiable"
    assert r["citations_total"] == 0


def test_verify_provenance_hash_cross_check():
    store = InMemoryVectorStore()
    store.ingest("d1", [
        {"id": "c1", "text": "x", "content_hash": "abc123def", "page_refs": [1]},
    ])
    citations = [{"content_hash": "abc123def", "page_number": 1}]
    r = verify_provenance(store, ["d1"], citations)
    assert r["status"] == "verified"
    assert r["citations_verified"] == 1 and r["citations_total"] == 1

    citations_bad = [{"content_hash": "not_in_store", "page_number": 1}]
    r2 = verify_provenance(store, ["d1"], citations_bad)
    assert r2["status"] == "unverifiable"
    assert r2["citations_verified"] == 0

    citations_mixed = [
        {"content_hash": "abc123def", "page_number": 1},
        {"content_hash": "missing", "page_number": 2},
    ]
    r3 = verify_provenance(store, ["d1"], citations_mixed)
    assert r3["status"] == "partial"
    assert r3["citations_verified"] == 1 and r3["citations_total"] == 2
