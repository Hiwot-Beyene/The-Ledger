from __future__ import annotations

import json

from pydantic import BaseModel, Field

from .common import StrategyName


class ExtractionLedgerEntry(BaseModel):
    timestamp: str
    doc_id: str
    document_name: str
    strategy_sequence: list[StrategyName]
    final_strategy: StrategyName
    confidence_score: float = Field(ge=0.0, le=1.0)
    cost_estimate_usd: float = Field(ge=0.0)
    processing_time_ms: int = Field(ge=0)
    budget_cap_usd: float = Field(ge=0.0)
    budget_status: str = Field(pattern="^(under_cap|cap_reached|approval_required)$")
    notes: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None

    def to_jsonl(self) -> str:
        return json.dumps(self.model_dump(), ensure_ascii=False)
