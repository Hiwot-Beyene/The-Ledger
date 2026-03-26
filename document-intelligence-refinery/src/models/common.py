from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class OriginType(str, Enum):
    NATIVE_DIGITAL = "native_digital"
    SCANNED_IMAGE = "scanned_image"
    MIXED = "mixed"
    FORM_FILLABLE = "form_fillable"


class LayoutComplexity(str, Enum):
    SINGLE_COLUMN = "single_column"
    MULTI_COLUMN = "multi_column"
    TABLE_HEAVY = "table_heavy"
    FIGURE_HEAVY = "figure_heavy"
    MIXED = "mixed"


class DomainHint(str, Enum):
    FINANCIAL = "financial"
    LEGAL = "legal"
    TECHNICAL = "technical"
    MEDICAL = "medical"
    GENERAL = "general"


class EstimatedExtractionCost(str, Enum):
    FAST_TEXT_SUFFICIENT = "fast_text_sufficient"
    NEEDS_LAYOUT_MODEL = "needs_layout_model"
    NEEDS_VISION_MODEL = "needs_vision_model"


class StrategyName(str, Enum):
    A = "A"
    B = "B"
    C = "C"


class ModelProvider(str, Enum):
    GEMINI = "gemini"
    OLLAMA = "ollama"
    OPENROUTER = "openrouter"
    OPENAI = "openai"


class ModelSelectionMode(str, Enum):
    AUTO = "auto"
    USER_OVERRIDE = "user_override"


class JobStage(str, Enum):
    TRIAGE = "triage"
    EXTRACTION = "extraction"
    CHUNKING = "chunking"
    INDEXING = "indexing"
    COMPLETED = "completed"
    FAILED = "failed"


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class LanguageInfo(BaseModel):
    code: str = Field(default="und")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
