"""
Token-based pricing for budget guard and cost tracking.
Supports OpenAI, OpenRouter, and Ollama (virtual pricing).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.models import ModelProvider

_DEFAULT_INPUT_PER_1M = 1.0
_DEFAULT_OUTPUT_PER_1M = 2.0

_pricing_cache: dict[str, Any] | None = None


def _load_pricing_table(rules: dict | None = None) -> dict[str, Any]:
    global _pricing_cache
    if _pricing_cache is not None:
        return _pricing_cache
    path = Path("rubric/pricing.yaml")
    if not path.exists():
        _pricing_cache = {"defaults": {"input_per_1m_usd": _DEFAULT_INPUT_PER_1M, "output_per_1m_usd": _DEFAULT_OUTPUT_PER_1M}}
        return _pricing_cache
    with open(path, encoding="utf-8") as f:
        _pricing_cache = yaml.safe_load(f) or {}
    return _pricing_cache


def get_model_pricing(
    provider: ModelProvider,
    model_name: str,
    rules: dict | None = None,
    runtime_overrides: dict | None = None,
) -> tuple[float, float]:
    """Return (input_per_1m_usd, output_per_1m_usd) for the given provider and model."""
    table = _load_pricing_table(rules)
    runtime_overrides = runtime_overrides or {}

    # User override from runtime (e.g. frontend)
    if runtime_overrides.get("ollama_input_per_1m_usd") is not None and provider == ModelProvider.OLLAMA:
        inp = float(runtime_overrides["ollama_input_per_1m_usd"])
        out = float(runtime_overrides.get("ollama_output_per_1m_usd") or inp * 2)
        return inp, out
    if runtime_overrides.get("input_per_1m_usd") is not None:
        inp = float(runtime_overrides["input_per_1m_usd"])
        out = float(runtime_overrides.get("output_per_1m_usd") or inp * 2)
        return inp, out

    provider_key = provider.value.lower()
    defaults = table.get("defaults", {})
    def_in = float(defaults.get("input_per_1m_usd", _DEFAULT_INPUT_PER_1M))
    def_out = float(defaults.get("output_per_1m_usd", _DEFAULT_OUTPUT_PER_1M))

    prov_cfg = table.get(provider_key, {})
    if not isinstance(prov_cfg, dict):
        return def_in, def_out
    models = prov_cfg.get("models") or {}
    model_entry = models.get(model_name) if isinstance(models, dict) else None
    if isinstance(model_entry, dict):
        return (
            float(model_entry.get("input_per_1m_usd", prov_cfg.get("input_per_1m_usd", def_in))),
            float(model_entry.get("output_per_1m_usd", prov_cfg.get("output_per_1m_usd", def_out))),
        )
    in_p = float(prov_cfg.get("input_per_1m_usd", def_in))
    out_p = float(prov_cfg.get("output_per_1m_usd", def_out))
    return in_p, out_p


def cost_from_usage(
    prompt_tokens: int,
    completion_tokens: int,
    provider: ModelProvider,
    model_name: str,
    rules: dict | None = None,
    runtime_overrides: dict | None = None,
) -> float:
    """Compute USD cost from token counts using pricing table (and virtual pricing for Ollama)."""
    in_p, out_p = get_model_pricing(provider, model_name, rules, runtime_overrides)
    return (prompt_tokens / 1_000_000.0) * in_p + (completion_tokens / 1_000_000.0) * out_p


def estimate_vision_cost_per_page(
    provider: ModelProvider,
    model_name: str,
    rules: dict | None = None,
    runtime_overrides: dict | None = None,
    prompt_tokens_per_page: int = 600,
    completion_tokens_per_page: int = 400,
) -> float:
    """Estimated USD cost for one vision page (prompt + image + typical output)."""
    return cost_from_usage(
        prompt_tokens_per_page,
        completion_tokens_per_page,
        provider,
        model_name,
        rules,
        runtime_overrides,
    )


def estimate_vision_run_cost(
    page_count: int,
    provider: ModelProvider,
    model_name: str,
    rules: dict | None = None,
    runtime_overrides: dict | None = None,
) -> float:
    """Total estimated USD cost for vision extraction over page_count pages."""
    per_page = estimate_vision_cost_per_page(
        provider, model_name, rules, runtime_overrides
    )
    return page_count * per_page
