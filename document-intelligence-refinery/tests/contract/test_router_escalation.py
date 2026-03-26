from pathlib import Path

from src.agents.extractor import ExtractionRouter
from src.models import (
    DocumentProfile,
    EstimatedExtractionCost,
    LanguageInfo,
    LayoutComplexity,
    OriginType,
    StrategyName,
    TriageSignals,
)
from src.models.extracted_document import ExtractedDocument, ExtractedMetadata
from src.strategies.base import ExtractionStrategy
from src.utils.rules import DEFAULT_RULES


class DummyStrategy(ExtractionStrategy):
    def __init__(self, name: StrategyName, confidence: float):
        self.name = name.value
        self._confidence = confidence

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
        return doc, self._confidence, 0.0


def _profile() -> DocumentProfile:
    return DocumentProfile(
        doc_id="doc123",
        document_name="sample.pdf",
        origin_type=OriginType.NATIVE_DIGITAL,
        layout_complexity=LayoutComplexity.SINGLE_COLUMN,
        language=LanguageInfo(code="en", confidence=0.9),
        domain_hint="general",
        estimated_extraction_cost=EstimatedExtractionCost.FAST_TEXT_SUFFICIENT,
        triage_signals=TriageSignals(
            avg_char_density=0.1,
            avg_whitespace_ratio=0.2,
            avg_image_area_ratio=0.1,
            table_density=0.0,
            figure_density=0.0,
        ),
        selected_strategy=StrategyName.A,
        triage_confidence_score=0.8,
    )


def test_router_escalates_a_to_b(tmp_path):
    router = ExtractionRouter(DEFAULT_RULES)
    router.strategies = {
        StrategyName.A: DummyStrategy(StrategyName.A, confidence=0.2),
        StrategyName.B: DummyStrategy(StrategyName.B, confidence=0.8),
        StrategyName.C: DummyStrategy(StrategyName.C, confidence=0.8),
    }
    pdf_path = tmp_path / "fake.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%%EOF")

    _, ledger = router.run(pdf_path, profile=_profile())
    assert ledger.strategy_sequence == [StrategyName.A, StrategyName.B]
    assert ledger.final_strategy == StrategyName.B


def test_router_escalates_b_to_c(tmp_path):
    profile = _profile()
    profile.selected_strategy = StrategyName.B

    router = ExtractionRouter(DEFAULT_RULES)
    router.strategies = {
        StrategyName.A: DummyStrategy(StrategyName.A, confidence=0.8),
        StrategyName.B: DummyStrategy(StrategyName.B, confidence=0.2),
        StrategyName.C: DummyStrategy(StrategyName.C, confidence=0.7),
    }
    pdf_path = tmp_path / "fake.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%%EOF")

    _, ledger = router.run(pdf_path, profile=profile)
    assert ledger.strategy_sequence == [StrategyName.B, StrategyName.C]
    assert ledger.final_strategy == StrategyName.C
