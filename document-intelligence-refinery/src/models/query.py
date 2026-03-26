from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field, model_validator

from .common import JobStage, JobStatus, ModelProvider, ModelSelectionMode
from .extracted_document import ProvenanceChain


class ModelSelectionDecision(BaseModel):
    decision_id: str = Field(min_length=6)
    provider: ModelProvider
    model_name: str = Field(min_length=1)
    mode: ModelSelectionMode = ModelSelectionMode.AUTO
    reasoning: str = Field(min_length=1)
    estimated_cost_usd: float = Field(default=0.0, ge=0.0)
    estimated_latency_ms: int = Field(default=0, ge=0)
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    doc_id: str | None = None
    query_id: str | None = None

    @model_validator(mode="after")
    def validate_scope(self) -> "ModelSelectionDecision":
        if not self.doc_id and not self.query_id:
            raise ValueError("ModelSelectionDecision requires doc_id or query_id")
        return self


class DocumentJobStatus(BaseModel):
    job_id: str = Field(min_length=6)
    doc_id: str = Field(min_length=4)
    stage: JobStage
    status: JobStatus
    progress_percent: int = Field(ge=0, le=100)
    message: str | None = None
    started_at: str | None = None
    updated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @model_validator(mode="after")
    def validate_completion(self) -> "DocumentJobStatus":
        if self.status == JobStatus.COMPLETED and self.stage != JobStage.COMPLETED:
            raise ValueError("completed status requires completed stage")
        return self


class QueryTraceRecord(BaseModel):
    query_id: str = Field(min_length=6)
    doc_ids: list[str] = Field(default_factory=list)
    tool_sequence: list[str] = Field(default_factory=list)
    model_decision: ModelSelectionDecision
    citations: list[ProvenanceChain] = Field(default_factory=list)
    langsmith_trace_id: str | None = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @model_validator(mode="after")
    def validate_tools(self) -> "QueryTraceRecord":
        if not self.tool_sequence:
            raise ValueError("QueryTraceRecord requires non-empty tool_sequence")
        return self
