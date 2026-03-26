from src.agents.chunker import validate_chunk, merge_ldus_for_ingestion
from src.models.extracted_document import LDU


def test_merge_ldus_combines_word_level_into_sentence():
    word_ldus = [
        {"id": "ldu-1", "text": "Customs", "content_hash": "a", "page_refs": [1], "parent_section": "p1", "provenance_chain": []},
        {"id": "ldu-2", "text": "law", "content_hash": "b", "page_refs": [1], "parent_section": "p1", "provenance_chain": []},
        {"id": "ldu-3", "text": "is", "content_hash": "c", "page_refs": [1], "parent_section": "p1", "provenance_chain": []},
        {"id": "ldu-4", "text": "governed", "content_hash": "d", "page_refs": [1], "parent_section": "p1", "provenance_chain": []},
        {"id": "ldu-5", "text": "by", "content_hash": "e", "page_refs": [1], "parent_section": "p1", "provenance_chain": []},
        {"id": "ldu-6", "text": "Proclamation", "content_hash": "f", "page_refs": [1], "parent_section": "p1", "provenance_chain": []},
        {"id": "ldu-7", "text": "No.", "content_hash": "g", "page_refs": [1], "parent_section": "p1", "provenance_chain": []},
        {"id": "ldu-8", "text": "859/2014.", "content_hash": "h", "page_refs": [1], "parent_section": "p1", "provenance_chain": []},
    ]
    merged = merge_ldus_for_ingestion(word_ldus)
    assert len(merged) <= 2
    combined = " ".join(m["text"] for m in merged)
    assert "859/2014" in combined
    assert "Proclamation" in combined
    assert all(m["page_refs"] == [1] for m in merged)


def test_chunk_validator_accepts_valid_ldu():
    ldu = LDU(
        id="ldu-1",
        text="Revenue increased",
        content_hash="abcd1234",
        parent_section="summary",
        page_refs=[1],
        provenance_chain=[
            {
                "document_name": "doc.pdf",
                "page_number": 1,
                "bbox": {"x0": 0, "y0": 0, "x1": 10, "y1": 10},
                "content_hash": "abcd1234",
            }
        ],
    )
    assert validate_chunk(ldu) == []


def test_chunk_validator_flags_missing_fields():
    ldu = LDU(
        id="ldu-2",
        text="",
        content_hash="abcd1234",
        parent_section=None,
        page_refs=[],
        provenance_chain=[],
    )
    issues = validate_chunk(ldu)
    assert "missing_page_refs" in issues
    assert "missing_parent_section" in issues
    assert "missing_provenance" in issues
