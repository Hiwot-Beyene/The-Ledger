from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


DEFAULT_RULES = {
    "triage": {
        "domain_classifier": "keyword",
        "native_min_char_count": 100,
        "scanned_max_char_count": 30,
        "scanned_min_image_ratio": 0.5,
        "scanned_pages_ratio_threshold": 0.85,
        "form_fillable_ratio_threshold": 0.20,
        "table_heavy_density_threshold": 0.15,
        "figure_heavy_density_threshold": 0.15,
        "multi_column_variation_threshold": 0.35,
        "single_column_max_table_density": 0.08,
        "single_column_max_variation": 0.20,
    },
    "confidence": {
        "escalate_threshold_ab": 0.45,
        "escalate_threshold_bc": 0.40,
    },
    "vision": {
        "max_cost_per_doc_usd": 20.0,
        "estimated_cost_per_page_usd": 0.02,
    },
    "model_selection": {
        "default_provider": "gemini",
        "default_model": "gemini-2.5-flash",
        "vision_provider": "gemini",
        "vision_model": "gemini-2.5-flash",
        "paid_provider": "gemini",
        "paid_model": "gemini-2.5-flash",
        "max_query_cost_usd": 0.20,
        "prefer_local_for_low_complexity": False,
    },
    "api": {
        "host": "0.0.0.0",
        "port": 8000,
        "status_poll_interval_ms": 2000,
    },
    "tracing": {
        "enabled": True,
        "required_metadata": [
            "query_id",
            "doc_id",
            "provider",
            "model",
            "tool_sequence",
            "citation_count",
        ],
    },
}


def deep_merge(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    merged = dict(left)
    for key, value in right.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_rules(rules_path: str | Path | None = None) -> dict[str, Any]:
    if not rules_path:
        return DEFAULT_RULES
    path = Path(rules_path)
    if not path.exists():
        return DEFAULT_RULES
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        return DEFAULT_RULES
    return deep_merge(DEFAULT_RULES, loaded)
