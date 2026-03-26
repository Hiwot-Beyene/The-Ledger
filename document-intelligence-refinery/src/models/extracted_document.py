from __future__ import annotations

import hashlib
from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator

from .common import StrategyName

ChunkType = Literal["paragraph", "table", "figure", "list", "heading", "mixed"]


class BBox(BaseModel):
    x0: float
    y0: float
    x1: float
    y1: float

    @model_validator(mode="after")
    def validate_order(self) -> "BBox":
        if self.x1 < self.x0 or self.y1 < self.y0:
            raise ValueError("bbox must satisfy x1>=x0 and y1>=y0")
        return self


class ProvenanceChain(BaseModel):
    document_name: str = Field(min_length=1)
    page_number: int = Field(ge=1)
    bbox: BBox
    content_hash: str = Field(min_length=8)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)


class LDU(BaseModel):
    id: str = Field(min_length=1)
    text: str = ""
    content_hash: str = Field(min_length=8)
    chunk_type: ChunkType = "paragraph"
    bounding_box: BBox | None = None
    token_count: int | None = None
    parent_section: str | None = None
    previous_chunk_id: str | None = None
    next_chunk_id: str | None = None
    reference_ids: list[str] = Field(default_factory=list)
    page_refs: list[int] = Field(default_factory=list)
    provenance_chain: list[ProvenanceChain] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_page_refs(self) -> "LDU":
        if any(page < 1 for page in self.page_refs):
            raise ValueError("page_refs must contain page numbers >= 1")
        return self


class PageIndexNode(BaseModel):
    id: str
    node_type: str
    label: str | None = None
    page_number: int | None = Field(default=None, ge=1)
    bbox: BBox | None = None
    children: list["PageIndexNode"] = Field(default_factory=list)


PageIndexNode.model_rebuild()


class TextBlock(BaseModel):
    id: str
    text: str
    bbox: BBox
    reading_order: int = Field(ge=0)


class TableObject(BaseModel):
    id: str
    title: Optional[str] = None
    headers: list[str] = Field(default_factory=list)
    rows: list[list[str]] = Field(default_factory=list)
    bbox: BBox
    reading_order: int = Field(default=0, ge=0)


class FigureObject(BaseModel):
    id: str
    caption: Optional[str] = None
    bbox: BBox
    references: list[str] = Field(default_factory=list)
    reading_order: int = Field(default=0, ge=0)


class ExtractedPage(BaseModel):
    page_number: int = Field(ge=1)
    width: float = Field(gt=0)
    height: float = Field(gt=0)
    text_blocks: list[TextBlock] = Field(default_factory=list)
    tables: list[TableObject] = Field(default_factory=list)
    figures: list[FigureObject] = Field(default_factory=list)
    ldu_ids: list[str] = Field(default_factory=list)


class ExtractedMetadata(BaseModel):
    source_strategy: StrategyName
    confidence_score: float = Field(ge=0.0, le=1.0)
    strategy_sequence: list[StrategyName] = Field(default_factory=list)
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


class ExtractedDocument(BaseModel):
    doc_id: str = Field(min_length=4)
    document_name: str = Field(min_length=1)
    pages: list[ExtractedPage]
    metadata: ExtractedMetadata
    ldus: list[LDU] = Field(default_factory=list)
    page_index: PageIndexNode | None = None
    provenance_chains: list[ProvenanceChain] = Field(default_factory=list)


def content_hash_for_text(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()


def estimate_token_count(text: str) -> int:
    if not text:
        return 0
    return max(1, int(len((text or "").split()) * 1.35))
