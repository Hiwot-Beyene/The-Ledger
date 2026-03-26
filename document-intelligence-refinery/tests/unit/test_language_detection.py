from src.utils.language import detect_language


def test_detect_language_fallback_for_english_text():
    code, confidence = detect_language(
        "This is a financial annual report with revenue numbers.")
    assert isinstance(code, str)
    assert 0.0 <= confidence <= 1.0


def test_detect_language_for_amharic_like_text():
    code, confidence = detect_language("ይህ የፋይናንስ ሪፖርት ነው")
    assert isinstance(code, str)
    assert 0.0 <= confidence <= 1.0
