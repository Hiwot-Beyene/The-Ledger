from __future__ import annotations

from pydantic import BaseModel, Field

from .common import (
    DomainHint,
    EstimatedExtractionCost,
    LanguageInfo,
    LayoutComplexity,
    OriginType,
    StrategyName,
)


class TriageSignals(BaseModel):
    avg_char_density: float = Field(ge=0.0)
    avg_whitespace_ratio: float = Field(ge=0.0, le=1.0)
    avg_image_area_ratio: float = Field(ge=0.0, le=1.0)
    table_density: float = Field(ge=0.0)
    figure_density: float = Field(ge=0.0)


class DocumentProfile(BaseModel):
    doc_id: str = Field(min_length=4)
    document_name: str = Field(min_length=1)
    origin_type: OriginType
    layout_complexity: LayoutComplexity
    language: LanguageInfo
    domain_hint: DomainHint
    estimated_extraction_cost: EstimatedExtractionCost
    triage_signals: TriageSignals
    selected_strategy: StrategyName
    triage_confidence_score: float = Field(ge=0.0, le=1.0)
