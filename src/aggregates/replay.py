"""Shared helpers for event replay: payload extraction, event type, version advance."""

from __future__ import annotations

from typing import Any

from models.events import StoredEvent


def payload_from_stored(event: StoredEvent | dict[str, Any]) -> dict[str, Any]:
    raw = getattr(event, "payload", None)
    if isinstance(raw, dict):
        return raw
    if isinstance(event, dict):
        return dict(event.get("payload", {}))
    return {}


def event_type_from_stored(event: StoredEvent | dict[str, Any]) -> str | None:
    et = getattr(event, "event_type", None)
    if et is None and isinstance(event, dict):
        return event.get("event_type")
    return et


def advance_version_from_stored(event: StoredEvent | dict[str, Any], current: int) -> int:
    stream_position = getattr(event, "stream_position", None)
    if stream_position is None:
        return current + 1
    return int(stream_position)


def rules_evaluate_ids(payload: dict[str, Any]) -> set[str]:
    return {str(rule) for rule in payload.get("rules_to_evaluate", [])}


def optional_float(payload: dict[str, Any], key: str) -> float | None:
    value = payload.get(key)
    return float(value) if value is not None else None


def optional_str(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    return str(value) if value is not None else None
