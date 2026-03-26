from __future__ import annotations

from src.agents.chunker import ChunkingEngine, build_ldus
from src.models import (
    BBox,
    ExtractedDocument,
    ExtractedMetadata,
    ExtractedPage,
    StrategyName,
    TableObject,
    TextBlock,
)


def test_chunking_engine_emits_table_ldu():
    doc = ExtractedDocument(
        doc_id="doc1",
        document_name="test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                width=612,
                height=792,
                text_blocks=[
                    TextBlock(
                        id="p1-t0",
                        text="Introduction",
                        bbox=BBox(x0=50, y0=50, x1=200, y1=70),
                        reading_order=0,
                    ),
                ],
                tables=[
                    TableObject(
                        id="p1-tbl0",
                        headers=["A", "B"],
                        rows=[["1", "2"]],
                        bbox=BBox(x0=50, y0=100, x1=200, y1=150),
                        reading_order=0,
                    ),
                ],
                figures=[],
            ),
        ],
        metadata=ExtractedMetadata(
            source_strategy=StrategyName.A,
            confidence_score=0.9,
            strategy_sequence=[StrategyName.A],
        ),
        ldus=[],
    )
    ldus = ChunkingEngine().build(doc)
    types = [u.chunk_type for u in ldus]
    assert "heading" in types or "paragraph" in types
    assert "table" in types
    table_ldu = next(u for u in ldus if u.chunk_type == "table")
    assert "|" in table_ldu.text
    assert "Columns:" in table_ldu.text or "1" in table_ldu.text
    assert table_ldu.token_count is not None
    assert table_ldu.bounding_box is not None
    assert table_ldu.parent_section


def test_table_semantic_format_keeps_label_and_value_together():
    doc = ExtractedDocument(
        doc_id="doc1",
        document_name="test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                width=612,
                height=792,
                text_blocks=[],
                tables=[
                    TableObject(
                        id="p1-tbl0",
                        headers=["Notes", "30 June 2022 Birr'ooo", "30 June 2021 Birr'ooo"],
                        rows=[
                            ["5", "Revenue from contracts with customers", "59,448,361", "55,499,196"],
                            ["", "Total comprehensive income for the year", "9,063,685", "5,651,046"],
                        ],
                        bbox=BBox(x0=50, y0=100, x1=400, y1=200),
                        reading_order=0,
                    ),
                ],
                figures=[],
            ),
        ],
        metadata=ExtractedMetadata(
            source_strategy=StrategyName.A,
            confidence_score=0.9,
            strategy_sequence=[StrategyName.A],
        ),
        ldus=[],
    )
    ldus = ChunkingEngine().build(doc)
    table_ldu = next(u for u in ldus if u.chunk_type == "table")
    assert "Total comprehensive income for the year:" in table_ldu.text
    assert "9,063,685" in table_ldu.text
    assert "Columns:" in table_ldu.text


def test_large_table_sectioned_with_headers_repeated():
    from src.agents.chunker import MAX_TABLE_ROWS_PER_CHUNK
    rows = [[f"Label{i}", "100", "200"] for i in range(MAX_TABLE_ROWS_PER_CHUNK + 5)]
    doc = ExtractedDocument(
        doc_id="doc1",
        document_name="test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                width=612,
                height=792,
                text_blocks=[],
                tables=[
                    TableObject(
                        id="p1-tbl0",
                        headers=["Item", "2022", "2021"],
                        rows=rows,
                        bbox=BBox(x0=0, y0=0, x1=600, y1=800),
                        reading_order=0,
                    ),
                ],
                figures=[],
            ),
        ],
        metadata=ExtractedMetadata(
            source_strategy=StrategyName.A,
            confidence_score=0.9,
            strategy_sequence=[StrategyName.A],
        ),
        ldus=[],
    )
    ldus = ChunkingEngine().build(doc)
    table_ldus = [u for u in ldus if u.chunk_type == "table"]
    assert len(table_ldus) >= 2
    for u in table_ldus:
        assert "Columns: Item | 2022 | 2021" in u.text


def test_build_ldus_from_pages_uses_engine():
    doc = ExtractedDocument(
        doc_id="doc1",
        document_name="test.pdf",
        pages=[
            ExtractedPage(
                page_number=1,
                width=612,
                height=792,
                text_blocks=[
                    TextBlock(
                        id="p1-t0",
                        text="1) First item",
                        bbox=BBox(x0=50, y0=50, x1=300, y1=70),
                        reading_order=0,
                    ),
                    TextBlock(
                        id="p1-t1",
                        text="2) Second item",
                        bbox=BBox(x0=50, y0=72, x1=300, y1=92),
                        reading_order=1,
                    ),
                ],
                tables=[],
                figures=[],
            ),
        ],
        metadata=ExtractedMetadata(
            source_strategy=StrategyName.A,
            confidence_score=0.9,
            strategy_sequence=[StrategyName.A],
        ),
        ldus=[],
    )
    ldus = build_ldus(doc)
    list_ldus = [u for u in ldus if u.chunk_type == "list"]
    assert len(list_ldus) >= 1
    assert "First item" in list_ldus[0].text and "Second item" in list_ldus[0].text
