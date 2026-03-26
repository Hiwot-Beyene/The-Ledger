from src.strategies.base import ScoreSignals, compute_confidence_score


def test_confidence_score_high_for_clean_digital_page():
    score = compute_confidence_score(
        ScoreSignals(char_count=400, char_density=0.003,
                     image_area_ratio=0.05, has_font_meta=1.0)
    )
    assert score > 0.7


def test_confidence_score_low_for_scanned_like_page():
    score = compute_confidence_score(
        ScoreSignals(char_count=5, char_density=0.00001,
                     image_area_ratio=0.95, has_font_meta=0.0)
    )
    assert score < 0.3


def test_confidence_score_mid_for_mixed_page_signals():
    score = compute_confidence_score(
        ScoreSignals(char_count=80, char_density=0.0008,
                     image_area_ratio=0.35, has_font_meta=0.5)
    )
    assert 0.3 <= score <= 0.8
