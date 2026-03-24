from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from models.events import StoredEvent

def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.now(UTC)


def _event_time(event: StoredEvent) -> datetime:
    payload = event.payload
    for key in (
        "submitted_at",
        "uploaded_at",
        "requested_at",
        "generated_at",
        "completed_at",
        "reviewed_at",
        "approved_at",
        "declined_at",
        "initiated_at",
        "evaluated_at",
        "detected_at",
        "written_at",
        "failed_at",
        "loaded_at",
        "started_at",
        "executed_at",
        "called_at",
    ):
        if key in payload and payload[key] is not None:
            return _dt(payload[key])
    return event.recorded_at


def _app_id(event: StoredEvent) -> str | None:
    aid = event.payload.get("application_id")
    if aid:
        return str(aid)
    parts = event.stream_id.split("-", 1)
    if len(parts) == 2 and parts[0] in {"loan", "credit", "compliance", "fraud", "docpkg"}:
        return parts[1]
    return None


