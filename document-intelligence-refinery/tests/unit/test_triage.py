from src.agents.triage import TriageAgent
from src.utils.rules import DEFAULT_RULES
from src.models.common import LayoutComplexity, OriginType, StrategyName


def test_origin_classification_native_digital():
    agent = TriageAgent(DEFAULT_RULES)
    origin = agent.classify_origin_type(
        avg_char_count=240, avg_image_ratio=0.10, scanned_pages_ratio=0.0, form_fillable_ratio=0.0)
    assert origin == OriginType.NATIVE_DIGITAL


def test_origin_classification_scanned_image():
    agent = TriageAgent(DEFAULT_RULES)
    origin = agent.classify_origin_type(
        avg_char_count=10, avg_image_ratio=0.8, scanned_pages_ratio=0.9, form_fillable_ratio=0.0)
    assert origin == OriginType.SCANNED_IMAGE


def test_origin_classification_mixed_mode_pdf():
    agent = TriageAgent(DEFAULT_RULES)
    origin = agent.classify_origin_type(
        avg_char_count=70, avg_image_ratio=0.45, scanned_pages_ratio=0.45, form_fillable_ratio=0.0)
    assert origin == OriginType.MIXED


def test_origin_classification_form_fillable_pdf():
    agent = TriageAgent(DEFAULT_RULES)
    origin = agent.classify_origin_type(
        avg_char_count=150, avg_image_ratio=0.05, scanned_pages_ratio=0.0, form_fillable_ratio=0.5)
    assert origin == OriginType.FORM_FILLABLE


def test_layout_classification_table_heavy():
    agent = TriageAgent(DEFAULT_RULES)
    layout = agent.classify_layout_complexity(
        table_density=0.2, figure_density=0.01, column_variation=0.1)
    assert layout == LayoutComplexity.TABLE_HEAVY


def test_layout_classification_multi_column():
    agent = TriageAgent(DEFAULT_RULES)
    layout = agent.classify_layout_complexity(
        table_density=0.01, figure_density=0.01, column_variation=0.5)
    assert layout == LayoutComplexity.MULTI_COLUMN


def test_origin_threshold_prefers_mixed_when_signals_ambiguous():
    agent = TriageAgent(DEFAULT_RULES)
    origin = agent.classify_origin_type(
        avg_char_count=95,
        avg_image_ratio=0.49,
        scanned_pages_ratio=0.4,
        form_fillable_ratio=0.0,
    )
    assert origin == OriginType.MIXED


def test_zero_text_confidence_guard_is_bounded():
    agent = TriageAgent(DEFAULT_RULES)
    confidence = agent.estimate_triage_confidence(
        avg_char_count=0.0,
        scanned_pages_ratio=0.0,
        form_fillable_ratio=0.0,
    )
    assert 0.0 <= confidence <= 1.0


def test_form_fillable_routes_to_strategy_c():
    agent = TriageAgent(DEFAULT_RULES)
    strategy = agent.select_strategy(
        OriginType.FORM_FILLABLE, LayoutComplexity.SINGLE_COLUMN)
    assert strategy == StrategyName.C


def test_mixed_layout_routes_to_strategy_b():
    agent = TriageAgent(DEFAULT_RULES)
    strategy = agent.select_strategy(OriginType.MIXED, LayoutComplexity.MIXED)
    assert strategy == StrategyName.B
