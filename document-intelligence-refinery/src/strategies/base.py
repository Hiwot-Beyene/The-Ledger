from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from src.models import DocumentProfile, ExtractedDocument


@dataclass
class ScoreSignals:
    char_count: float
    char_density: float
    image_area_ratio: float
    has_font_meta: float


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def compute_confidence_score(signals: ScoreSignals) -> float:
    char_signal = _clamp(signals.char_count / 200.0)
    density_signal = _clamp(signals.char_density / 0.002)
    image_penalty = _clamp(1.0 - signals.image_area_ratio)
    font_signal = _clamp(signals.has_font_meta)
    return _clamp(
        0.35 * char_signal + 0.30 * density_signal +
        0.25 * image_penalty + 0.10 * font_signal
    )


class ExtractionStrategy(ABC):
    name: str

    @abstractmethod
    def extract(self, pdf_path: Path, profile: DocumentProfile, rules: dict) -> tuple[ExtractedDocument, float, float]:
        """Return (extracted_document, confidence_score, cost_estimate_usd)."""
