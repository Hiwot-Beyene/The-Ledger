"""
Map DoclingDocument (from docling.document_converter) to our ExtractedDocument.
Strategy B — Layout-Aware per challenge: MinerU or Docling.
"""
from __future__ import annotations

from pathlib import Path

from src.models import (
    BBox,
    DocumentProfile,
    ExtractedDocument,
    ExtractedMetadata,
    ExtractedPage,
    LDU,
    PageIndexNode,
    ProvenanceChain,
    StrategyName,
    TableObject,
    TextBlock,
    content_hash_for_text,
)


def _bbox_from_docling(bbox) -> BBox:
    if bbox is None:
        return BBox(x0=0.0, y0=0.0, x1=0.0, y1=0.0)
    l = getattr(bbox, "l", 0.0) or 0.0
    t = getattr(bbox, "t", 0.0) or 0.0
    r = getattr(bbox, "r", 0.0) or 0.0
    b = getattr(bbox, "b", 0.0) or 0.0
    return BBox(x0=float(l), y0=float(t), x1=float(r), y1=float(b))


def _page_size(doc, page_no: int) -> tuple[float, float]:
    pages = getattr(doc, "pages", None) or {}
    page_item = pages.get(page_no) if isinstance(pages, dict) else None
    if page_item is not None:
        size = getattr(page_item, "size", None)
        if size is not None and len(size) >= 2:
            return (float(size[0]), float(size[1]))
    return (612.0, 792.0)


def docling_document_to_extracted(doc, profile: DocumentProfile) -> ExtractedDocument:
    """Convert a DoclingDocument to ExtractedDocument. doc is from ConversionResult.document."""
    from docling_core.types.doc import DoclingDocument

    if not isinstance(doc, DoclingDocument):
        raise TypeError("doc must be DoclingDocument")

    pages_map: dict[int, dict] = {}
    all_ldus: list[LDU] = []
    provenance_chains: list[ProvenanceChain] = []
    reading_order = 0

    texts = getattr(doc, "texts", []) or []
    for i, item in enumerate(texts):
        text = (getattr(item, "text", None) or "").strip()
        if not text:
            continue
        prov_list = getattr(item, "prov", None) or []
        if not prov_list:
            continue
        p0 = prov_list[0]
        page_no = int(getattr(p0, "page_no", 1) or 1)
        bbox = _bbox_from_docling(getattr(p0, "bbox", None))
        block_id = f"p{page_no}-dl-t{i}"
        reading_order += 1
        tb = TextBlock(id=block_id, text=text, bbox=bbox, reading_order=reading_order)
        if page_no not in pages_map:
            pages_map[page_no] = {"text_blocks": [], "tables": [], "figures": []}
        pages_map[page_no]["text_blocks"].append(tb)
        ch = content_hash_for_text(text)
        chain = ProvenanceChain(
            document_name=profile.document_name,
            page_number=page_no,
            bbox=bbox,
            content_hash=ch,
        )
        provenance_chains.append(chain)
        ldu = LDU(
            id=f"ldu-{block_id}",
            text=text,
            content_hash=ch,
            parent_section=f"page_{page_no}",
            page_refs=[page_no],
            provenance_chain=[chain],
        )
        all_ldus.append(ldu)

    tables = getattr(doc, "tables", []) or []
    for ti, table in enumerate(tables):
        prov_list = getattr(table, "prov", None) or []
        page_no = 1
        bbox = BBox(x0=0.0, y0=0.0, x1=0.0, y1=0.0)
        if prov_list:
            p0 = prov_list[0]
            page_no = int(getattr(p0, "page_no", 1) or 1)
            bbox = _bbox_from_docling(getattr(p0, "bbox", None))
        data = getattr(table, "data", None)
        headers: list[str] = []
        rows: list[list[str]] = []
        if data is not None:
            num_rows = int(getattr(data, "num_rows", 0) or 0)
            num_cols = int(getattr(data, "num_cols", 0) or 0)
            cells = getattr(data, "table_cells", []) or []
            if num_rows and num_cols and cells:
                grid: list[list[str]] = [[""] * num_cols for _ in range(num_rows)]
                for cell in cells:
                    r = int(getattr(cell, "start_row_offset_idx", 0) or 0)
                    c = int(getattr(cell, "start_col_offset_idx", 0) or 0)
                    text = (getattr(cell, "text", None) or "").strip()
                    if r < num_rows and c < num_cols:
                        grid[r][c] = text
                if grid:
                    headers = grid[0]
                    rows = grid[1:]
            elif isinstance(data, list) and len(data) > 0:
                first = data[0]
                if isinstance(first, (list, tuple)):
                    headers = [str(x) for x in first]
                    for r in data[1:]:
                        rows.append([str(x) for x in (list(r) if isinstance(r, (list, tuple)) else [])])
        if page_no not in pages_map:
            pages_map[page_no] = {"text_blocks": [], "tables": [], "figures": []}
        pages_map[page_no]["tables"].append(
            TableObject(
                id=f"p{page_no}-dl-tbl{ti}",
                title=None,
                headers=headers,
                rows=rows,
                bbox=bbox,
                reading_order=ti,
            )
        )

    page_numbers = sorted(pages_map.keys())
    extracted_pages: list[ExtractedPage] = []
    page_nodes: list[PageIndexNode] = []
    for pno in page_numbers:
        w, h = _page_size(doc, pno)
        data = pages_map[pno]
        text_blocks = data["text_blocks"]
        table_objs = data["tables"]
        figures = data["figures"]
        page_ldu_ids = [ldu.id for ldu in all_ldus if ldu.page_refs and pno in ldu.page_refs]
        extracted_pages.append(
            ExtractedPage(
                page_number=pno,
                width=w,
                height=h,
                text_blocks=text_blocks,
                tables=table_objs,
                figures=figures,
                ldu_ids=page_ldu_ids,
            )
        )
        block_nodes = [
            PageIndexNode(id=tb.id, node_type="text_block", label=(tb.text[:80] if tb.text else None), page_number=pno, bbox=tb.bbox, children=[])
            for tb in text_blocks
        ]
        page_nodes.append(
            PageIndexNode(id=f"page-{pno}", node_type="page", label=f"Page {pno}", page_number=pno, bbox=BBox(x0=0.0, y0=0.0, x1=w, y1=h), children=block_nodes)
        )

    for idx, ldu in enumerate(all_ldus):
        if idx > 0:
            ldu.previous_chunk_id = all_ldus[idx - 1].id
        if idx + 1 < len(all_ldus):
            ldu.next_chunk_id = all_ldus[idx + 1].id

    root = PageIndexNode(
        id=f"doc-{profile.doc_id}",
        node_type="document",
        label=profile.document_name,
        children=page_nodes,
    )
    confidence = 0.75 if (texts or tables) else 0.4
    return ExtractedDocument(
        doc_id=profile.doc_id,
        document_name=profile.document_name,
        pages=extracted_pages,
        metadata=ExtractedMetadata(
            source_strategy=StrategyName.B,
            confidence_score=min(1.0, confidence),
            strategy_sequence=[StrategyName.B],
        ),
        ldus=all_ldus,
        page_index=root,
        provenance_chains=provenance_chains,
    )


def run_docling(pdf_path: Path, profile: DocumentProfile) -> ExtractedDocument | None:
    """Run Docling on PDF and return ExtractedDocument, or None if Docling is unavailable or fails."""
    try:
        from docling.document_converter import DocumentConverter
    except Exception:
        return None
    try:
        converter = DocumentConverter()
        result = converter.convert(str(pdf_path))
        doc = result.document
        return docling_document_to_extracted(doc, profile)
    except Exception:
        return None
