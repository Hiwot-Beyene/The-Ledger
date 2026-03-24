"""Registered upcasters: CreditAnalysisCompleted v1→v2, DecisionGenerated v1→v2."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def _as_utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return datetime(1970, 1, 1, tzinfo=UTC)


def _infer_credit_model_version(recorded_at: datetime) -> str:
    if recorded_at < datetime(2025, 1, 1, tzinfo=UTC):
        return "legacy-pre-2025"
    if recorded_at < datetime(2026, 1, 1, tzinfo=UTC):
        return "legacy-2025"
    return "legacy-pre-2026"


def _infer_regulatory_basis(recorded_at: datetime) -> list[str]:
    if recorded_at < datetime(2025, 1, 1, tzinfo=UTC):
        return ["REG-BASELINE-2024"]
    if recorded_at < datetime(2026, 1, 1, tzinfo=UTC):
        return ["REG-BASELINE-2025-Q4"]
    return ["REG-BASELINE-2026-Q1"]


def register_builtin_upcasters(reg: Any) -> None:
    @reg.register("CreditAnalysisCompleted", 1)
    def _credit_v1_to_v2(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        envelope = context.get("event", {})
        recorded_at = envelope.get("recorded_at")
        ts = _as_utc_datetime(recorded_at)
        upgraded = dict(payload)
        upgraded.setdefault("model_version", _infer_credit_model_version(ts))
        upgraded.setdefault("confidence_score", None)
        upgraded.setdefault("regulatory_basis", _infer_regulatory_basis(ts))
        return upgraded

    @reg.register("DecisionGenerated", 1)
    def _decision_v1_to_v2(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        upgraded = dict(payload)
        inferred: dict[str, Any] = dict(context.get("decision_model_versions", {}) or {})
        merged = dict(upgraded.get("model_versions") or {})
        merged.update(inferred)
        upgraded["model_versions"] = merged
        return upgraded
