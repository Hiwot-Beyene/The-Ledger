import pytest

from src.models import (
    DocumentProfile,
    DomainHint,
    EstimatedExtractionCost,
    LanguageInfo,
    LayoutComplexity,
    OriginType,
    TriageSignals,
)
from src.strategies.vision import _get_tesseract_lang, _get_surya_lang_codes


def _profile(lang_code: str = "und", confidence: float = 0.0) -> DocumentProfile:
    return DocumentProfile(
        doc_id="doc-ocr",
        document_name="doc.pdf",
        origin_type=OriginType.SCANNED_IMAGE,
        layout_complexity=LayoutComplexity.MIXED,
        language=LanguageInfo(code=lang_code, confidence=confidence),
        domain_hint=DomainHint.GENERAL,
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


def test_get_tesseract_lang_amharic():
    rules = {"vision": {"default_ocr_lang": "eng"}}
    assert _get_tesseract_lang(_profile("am", 0.9), rules) == "amh+eng"


def test_get_tesseract_lang_und_uses_eng():
    """Unknown language always uses English for efficient scanned-doc handling."""
    rules = {"vision": {"default_ocr_lang": "amh"}}
    assert _get_tesseract_lang(_profile("und", 0.0), rules) == "eng"


def test_get_tesseract_lang_english():
    rules = {"vision": {}}
    assert _get_tesseract_lang(_profile("en", 0.8), rules) == "eng"


def test_get_tesseract_lang_english_low_conf_still_eng():
    rules = {"vision": {"default_ocr_lang": "amh"}}
    p = _profile("en", 0.5)
    assert _get_tesseract_lang(p, rules) == "eng"


def test_get_surya_lang_codes_amharic():
    rules = {"vision": {}}
    assert _get_surya_lang_codes(_profile("am", 0.9), rules) == ["am"]


def test_get_surya_lang_codes_und_uses_default_amh():
    rules = {"vision": {"default_ocr_lang": "amh"}}
    assert _get_surya_lang_codes(_profile("und", 0.0), rules) == ["am", "en"]


def test_get_surya_lang_codes_english():
    rules = {"vision": {}}
    assert _get_surya_lang_codes(_profile("en", 0.8), rules) == ["en"]
