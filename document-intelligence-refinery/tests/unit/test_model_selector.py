from src.models import (
    DocumentProfile,
    EstimatedExtractionCost,
    LanguageInfo,
    LayoutComplexity,
    OriginType,
    TriageSignals,
)
from src.services.model_gateway import ModelGateway
from src.utils.rules import DEFAULT_RULES


def _scanned_profile() -> DocumentProfile:
    return DocumentProfile(
        doc_id="doc1234",
        document_name="scan.pdf",
        origin_type=OriginType.SCANNED_IMAGE,
        layout_complexity=LayoutComplexity.MIXED,
        language=LanguageInfo(code="en", confidence=0.8),
        domain_hint="general",
        estimated_extraction_cost=EstimatedExtractionCost.NEEDS_VISION_MODEL,
        triage_signals=TriageSignals(
            avg_char_density=0.001,
            avg_whitespace_ratio=0.2,
            avg_image_area_ratio=0.8,
            table_density=0.1,
            figure_density=0.2,
        ),
        selected_strategy="C",
        triage_confidence_score=0.7,
    )


def test_recommend_prefers_vision_for_scanned_profile():
    gateway = ModelGateway(DEFAULT_RULES)
    provider, model, reason = gateway.recommend(profile=_scanned_profile(), query="summarize")
    assert provider.value == "ollama"
    assert model
    assert "vision" in reason


def test_select_model_uses_override_when_present():
    gateway = ModelGateway(DEFAULT_RULES)
    decision = gateway.select_model(
        query="What is total revenue?",
        override={"provider": "ollama", "model_name": "llama3.1:70b"},
        query_id="q-123456",
    )
    assert decision.provider.value == "ollama"
    assert decision.model_name == "llama3.1:70b"
    assert decision.mode.value == "user_override"
