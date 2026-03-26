"""
Constitution rule validation and negative tests: invalid LDUs are rejected.
"""
from __future__ import annotations

import pytest

from src.agents.chunker import (
    MAX_LIST_TOKENS,
    assert_constitution,
    validate_chunk,
    validate_ldus_constitution,
)
from src.models.extracted_document import BBox, LDU, ProvenanceChain


def _provenance() -> list[ProvenanceChain]:
    return [
        ProvenanceChain(
            document_name="doc.pdf",
            page_number=1,
            bbox=BBox(x0=0, y0=0, x1=10, y1=10),
            content_hash="abcd1234",
        )
    ]


def test_table_ldu_without_header_context_rejected():
    ldu = LDU(
        id="ldu-tbl1",
        text="only one row value",
        content_hash="a" * 8,
        chunk_type="table",
        parent_section="Section",
        page_refs=[1],
        provenance_chain=_provenance(),
    )
    issues = validate_chunk(ldu)
    assert "table_chunk_missing_header_context" in issues


def test_table_ldu_empty_text_rejected():
    ldu = LDU(
        id="ldu-tbl2",
        text="",
        content_hash="b" * 8,
        chunk_type="table",
        parent_section="Section",
        page_refs=[1],
        provenance_chain=_provenance(),
    )
    issues = validate_chunk(ldu)
    assert "table_chunk_empty" in issues


def test_table_ldu_with_columns_line_accepted():
    ldu = LDU(
        id="ldu-tbl3",
        text="Columns: A | B\nRow1: 1 | 2",
        content_hash="c" * 8,
        chunk_type="table",
        parent_section="Section",
        page_refs=[1],
        provenance_chain=_provenance(),
    )
    issues = validate_chunk(ldu)
    assert not [i for i in issues if i.startswith("table_")]


def test_figure_ldu_empty_text_rejected():
    ldu = LDU(
        id="ldu-fig1",
        text="",
        content_hash="d" * 8,
        chunk_type="figure",
        parent_section="Section",
        page_refs=[1],
        provenance_chain=_provenance(),
    )
    issues = validate_chunk(ldu)
    assert "figure_chunk_missing_caption" in issues


def test_list_ldu_exceeding_max_tokens_rejected():
    ldu = LDU(
        id="ldu-list1",
        text="1) First\n2) Second",
        content_hash="e" * 8,
        chunk_type="list",
        parent_section="Section",
        page_refs=[1],
        token_count=MAX_LIST_TOKENS + 100,
        provenance_chain=_provenance(),
    )
    issues = validate_chunk(ldu, max_list_tokens=MAX_LIST_TOKENS)
    assert "list_chunk_exceeds_max_tokens" in issues


def test_list_ldu_under_max_tokens_accepted():
    ldu = LDU(
        id="ldu-list2",
        text="1) First\n2) Second",
        content_hash="f" * 8,
        chunk_type="list",
        parent_section="Section",
        page_refs=[1],
        token_count=10,
        provenance_chain=_provenance(),
    )
    issues = validate_chunk(ldu, max_list_tokens=MAX_LIST_TOKENS)
    assert "list_chunk_exceeds_max_tokens" not in issues


def test_validate_ldus_constitution_returns_violations():
    valid = LDU(
        id="ok",
        text="OK",
        content_hash="g" * 8,
        parent_section="S",
        page_refs=[1],
        provenance_chain=_provenance(),
    )
    invalid_table = LDU(
        id="bad-table",
        text="no header",
        content_hash="h" * 8,
        chunk_type="table",
        parent_section="S",
        page_refs=[1],
        provenance_chain=_provenance(),
    )
    violations = validate_ldus_constitution([valid, invalid_table])
    assert len(violations) >= 1
    ids = [v[0] for v in violations]
    assert "bad-table" in ids
    assert any("table" in v[1] or "table_chunk" in v[2] for v in violations)


def test_assert_constitution_raises_on_violations():
    invalid = LDU(
        id="bad",
        text="",
        content_hash="i" * 8,
        chunk_type="figure",
        parent_section="S",
        page_refs=[1],
        provenance_chain=_provenance(),
    )
    with pytest.raises(AssertionError) as exc_info:
        assert_constitution([invalid])
    assert "figure_chunk_missing_caption" in str(exc_info.value) or "Chunking constitution" in str(exc_info.value)


def test_assert_constitution_passes_valid_ldus():
    valid = LDU(
        id="v",
        text="Caption",
        content_hash="j" * 8,
        chunk_type="figure",
        parent_section="S",
        page_refs=[1],
        provenance_chain=_provenance(),
    )
    assert_constitution([valid])


def test_engine_emitted_ldus_pass_constitution():
    from src.agents.chunker import ChunkingEngine
    from src.models import ExtractedDocument, ExtractedMetadata, ExtractedPage, StrategyName, TableObject, TextBlock

    doc = ExtractedDocument(
        doc_id="doc1",
        document_name="d.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                width=612,
                height=792,
                text_blocks=[
                    TextBlock(id="t1", text="Intro", bbox=BBox(x0=0, y0=0, x1=10, y1=10), reading_order=0),
                ],
                tables=[
                    TableObject(id="tbl1", headers=["A"], rows=[["1"]], bbox=BBox(x0=0, y0=0, x1=10, y1=10), reading_order=0),
                ],
                figures=[],
            ),
        ],
        metadata=ExtractedMetadata(source_strategy=StrategyName.A, confidence_score=0.9, strategy_sequence=[StrategyName.A]),
        ldus=[],
    )
    ldus = ChunkingEngine().build(doc)
    assert_constitution(ldus)
