from __future__ import annotations

import json
from pathlib import Path

from src.agents.extractor import ExtractionRouter
from src.models import (
    DocumentProfile,
    EstimatedExtractionCost,
    ExtractionLedgerEntry,
    LanguageInfo,
    LayoutComplexity,
    OriginType,
    StrategyName,
    TriageSignals,
)
from src.models.extracted_document import ExtractedDocument, ExtractedMetadata
from src.strategies.base import ExtractionStrategy
from src.utils.rules import DEFAULT_RULES


class _ConstantStrategy(ExtractionStrategy):
    def __init__(self, name: StrategyName, confidence: float, cost: float) -> None:
        self.name = name.value
        self._confidence = confidence
        self._cost = cost

    def extract(self, pdf_path: Path, profile: DocumentProfile, rules: dict):
        doc = ExtractedDocument(
            doc_id=profile.doc_id,
            document_name=profile.document_name,
            pages=[],
            metadata=ExtractedMetadata(
                source_strategy=StrategyName(self.name),
                confidence_score=self._confidence,
                strategy_sequence=[StrategyName(self.name)],
            ),
        )
        return doc, self._confidence, self._cost


def _profile() -> DocumentProfile:
    return DocumentProfile(
        doc_id="doc-ledger-001",
        document_name="ledger.pdf",
        origin_type=OriginType.NATIVE_DIGITAL,
        layout_complexity=LayoutComplexity.SINGLE_COLUMN,
        language=LanguageInfo(code="en", confidence=0.9),
        domain_hint="general",
        estimated_extraction_cost=EstimatedExtractionCost.FAST_TEXT_SUFFICIENT,
        triage_signals=TriageSignals(
            avg_char_density=0.1,
            avg_whitespace_ratio=0.2,
            avg_image_area_ratio=0.0,
            table_density=0.0,
            figure_density=0.0,
        ),
        selected_strategy=StrategyName.A,
        triage_confidence_score=0.9,
    )


def test_extraction_writes_jsonl_ledger(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".refinery").mkdir(parents=True, exist_ok=True)

    router = ExtractionRouter(DEFAULT_RULES)
    router.strategies = {
        StrategyName.A: _ConstantStrategy(StrategyName.A, confidence=0.8, cost=0.0),
        StrategyName.B: _ConstantStrategy(StrategyName.B, confidence=0.8, cost=0.01),
        StrategyName.C: _ConstantStrategy(StrategyName.C, confidence=0.8, cost=0.02),
    }

    pdf_path = tmp_path / "fake.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%%EOF")

    _, ledger = router.run(pdf_path, profile=_profile())
    assert isinstance(ledger, ExtractionLedgerEntry)

    ledger_path = tmp_path / ".refinery" / "extraction_ledger.jsonl"
    assert ledger_path.exists()

    lines = [line for line in ledger_path.read_text(encoding="utf-8").splitlines() if line]
    assert len(lines) == 1

    payload = json.loads(lines[0])
    assert payload["doc_id"] == "doc-ledger-001"
    assert payload["final_strategy"] == "A"
    assert payload["budget_status"] in {"under_cap", "cap_reached", "approval_required"}
