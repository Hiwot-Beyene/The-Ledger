from __future__ import annotations

from pathlib import Path

import pdfplumber

from src.models import (
    BBox,
    DocumentProfile,
    ExtractedDocument,
    ExtractedMetadata,
    ExtractedPage,
    FigureObject,
    LDU,
    PageIndexNode,
    ProvenanceChain,
    StrategyName,
    TableObject,
    TextBlock,
    content_hash_for_text,
)
from src.strategies.base import ExtractionStrategy, ScoreSignals, compute_confidence_score


class FastTextExtractor(ExtractionStrategy):
    name = "A"

    @staticmethod
    def _image_area(image: dict) -> float:
        x0 = image.get("x0", 0) or 0
        x1 = image.get("x1", 0) or 0
        top = image.get("top", 0) or 0
        bottom = image.get("bottom", 0) or 0
        return max(0.0, (x1 - x0)) * max(0.0, (bottom - top))

    def extract(self, pdf_path: Path, profile: DocumentProfile, rules: dict) -> tuple[ExtractedDocument, float, float]:
        pages: list[ExtractedPage] = []
        page_scores: list[float] = []
        ldus: list[LDU] = []
        provenance_chains: list[ProvenanceChain] = []
        page_nodes: list[PageIndexNode] = []

        with pdfplumber.open(str(pdf_path)) as pdf:
            for page_no, page in enumerate(pdf.pages, start=1):
                words = page.extract_words() or []
                page_ldu_ids: list[str] = []
                block_nodes: list[PageIndexNode] = []
                text_blocks = [
                    TextBlock(
                        id=f"p{page_no}-w{i}",
                        text=w.get("text", ""),
                        bbox=BBox(
                            x0=float(w.get("x0", 0)),
                            y0=float(w.get("top", 0)),
                            x1=float(w.get("x1", 0)),
                            y1=float(w.get("bottom", 0)),
                        ),
                        reading_order=i,
                    )
                    for i, w in enumerate(words)
                ]

                for block in text_blocks:
                    content_hash = content_hash_for_text(block.text)
                    chain = ProvenanceChain(
                        document_name=profile.document_name,
                        page_number=page_no,
                        bbox=block.bbox,
                        content_hash=content_hash,
                    )
                    provenance_chains.append(chain)
                    ldu = LDU(
                        id=f"ldu-{block.id}",
                        text=block.text,
                        content_hash=content_hash,
                        parent_section=f"page_{page_no}",
                        page_refs=[page_no],
                        provenance_chain=[chain],
                    )
                    ldus.append(ldu)
                    page_ldu_ids.append(ldu.id)
                    block_nodes.append(
                        PageIndexNode(
                            id=block.id,
                            node_type="text_block",
                            label=(block.text[:80] if block.text else None),
                            page_number=page_no,
                            bbox=block.bbox,
                            children=[],
                        )
                    )

                for idx, ldu in enumerate(ldus):
                    if idx > 0 and ldu.id.startswith(f"ldu-p{page_no}-"):
                        previous_id = ldus[idx - 1].id
                        if previous_id.startswith(f"ldu-p{page_no}-"):
                            ldu.previous_chunk_id = previous_id
                            ldus[idx - 1].next_chunk_id = ldu.id

                tables = []
                for t_idx, table in enumerate(page.extract_tables() or []):
                    if not table:
                        continue
                    headers = [str(h or "") for h in (table[0] or [])]
                    rows = [[str(c or "") for c in row] for row in table[1:]]
                    tables.append(
                        TableObject(
                            id=f"p{page_no}-t{t_idx}",
                            title=None,
                            headers=headers,
                            rows=rows,
                            bbox=BBox(x0=0.0, y0=0.0, x1=float(
                                page.width), y1=float(page.height)),
                            reading_order=t_idx,
                        )
                    )

                figures = [
                    FigureObject(
                        id=f"p{page_no}-f{i}",
                        caption=None,
                        bbox=BBox(
                            x0=float(img.get("x0", 0) or 0),
                            y0=float(img.get("top", 0) or 0),
                            x1=float(img.get("x1", 0) or 0),
                            y1=float(img.get("bottom", 0) or 0),
                        ),
                        references=[],
                        reading_order=i,
                    )
                    for i, img in enumerate(page.images or [])
                ]

                chars = page.chars or []
                page_area = max(float(page.width * page.height), 1.0)
                char_count = float(len(chars))
                char_density = char_count / page_area
                image_area_ratio = sum(self._image_area(image)
                                       for image in (page.images or [])) / page_area
                has_font_meta = 1.0 if any(c.get("fontname")
                                           for c in chars) else 0.0
                page_scores.append(
                    compute_confidence_score(
                        ScoreSignals(
                            char_count=char_count,
                            char_density=char_density,
                            image_area_ratio=min(image_area_ratio, 1.0),
                            has_font_meta=has_font_meta,
                        )
                    )
                )

                pages.append(
                    ExtractedPage(
                        page_number=page_no,
                        width=float(page.width),
                        height=float(page.height),
                        text_blocks=text_blocks,
                        tables=tables,
                        figures=figures,
                        ldu_ids=page_ldu_ids,
                    )
                )

                page_nodes.append(
                    PageIndexNode(
                        id=f"page-{page_no}",
                        node_type="page",
                        label=f"Page {page_no}",
                        page_number=page_no,
                        bbox=BBox(x0=0.0, y0=0.0, x1=float(
                            page.width), y1=float(page.height)),
                        children=block_nodes,
                    )
                )

        confidence = sum(page_scores) / max(len(page_scores), 1)
        extracted = ExtractedDocument(
            doc_id=profile.doc_id,
            document_name=profile.document_name,
            pages=pages,
            metadata=ExtractedMetadata(
                source_strategy=StrategyName.A,
                confidence_score=confidence,
                strategy_sequence=[StrategyName.A],
            ),
            ldus=ldus,
            page_index=PageIndexNode(
                id=f"doc-{profile.doc_id}",
                node_type="document",
                label=profile.document_name,
                children=page_nodes,
            ),
            provenance_chains=provenance_chains,
        )
        return extracted, confidence, 0.0
