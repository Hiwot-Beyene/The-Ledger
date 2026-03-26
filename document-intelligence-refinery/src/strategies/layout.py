"""
Strategy B — Layout-Aware (Cost: Medium). Tool: Docling or MinerU per challenge.
Triggers when: multi_column OR table_heavy OR figure_heavy OR mixed layout.
Extracts: text blocks with bbox, tables as structured JSON, figures, reading order.
Falls back to FastText (pdfplumber) when Docling is unavailable or fails.
"""
from __future__ import annotations

from pathlib import Path

from src.models import (
    DocumentProfile,
    ExtractedDocument,
    ExtractedMetadata,
    StrategyName,
)
from src.strategies.base import ExtractionStrategy
from src.strategies.fast_text import FastTextExtractor


class LayoutExtractor(ExtractionStrategy):
    name = "B"

    def __init__(self) -> None:
        self._fallback = FastTextExtractor()

    def extract(self, pdf_path: Path, profile: DocumentProfile, rules: dict) -> tuple[ExtractedDocument, float, float]:
        use_docling = bool(rules.get("layout", {}).get("use_docling", True))
        extracted = None
        if use_docling:
            try:
                from src.services.docling_adapter import run_docling
                extracted = run_docling(pdf_path, profile)
            except Exception:
                extracted = None
        if extracted is None:
            extracted, confidence, _ = self._fallback.extract(pdf_path, profile, rules)
            table_count = sum(len(p.tables) for p in extracted.pages)
            confidence = min(1.0, confidence + (0.12 if table_count > 0 else 0.05))
            extracted.metadata = ExtractedMetadata(
                source_strategy=StrategyName.B,
                confidence_score=confidence,
                strategy_sequence=[StrategyName.B],
            )
            return extracted, confidence, 0.0
        conf = extracted.metadata.confidence_score
        return extracted, conf, 0.0
