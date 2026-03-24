from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Protocol

from models.events import StoredEvent


class EventStoreLike(Protocol):
    async def load_stream(self, stream_id: str, from_position: int = 0) -> list[StoredEvent]: ...


@dataclass(slots=True)
class AgentContext:
    context_text: str
    last_event_position: int
    pending_work: list[str] = field(default_factory=list)
    session_health_status: str = "HEALTHY"


def _event_line(event: StoredEvent) -> str:
    return (
        f"[{event.stream_position}] {event.event_type} "
        f"payload={event.payload} at={event.recorded_at.isoformat()}"
    )


def _trim_to_budget(text: str, token_budget: int) -> str:
    max_chars = max(256, token_budget * 4)
    return text if len(text) <= max_chars else text[-max_chars:]


async def reconstruct_agent_context(
    store: EventStoreLike,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    stream_id = f"agent-{agent_id}-{session_id}"
    events = await store.load_stream(stream_id)
    if not events:
        return AgentContext(
            context_text="No session events found.",
            last_event_position=0,
            pending_work=["Start session with AgentContextLoaded"],
            session_health_status="NEEDS_RECONCILIATION",
        )

    counts = Counter(event.event_type for event in events[:-3])
    summary = "; ".join(f"{k}={v}" for k, v in sorted(counts.items()))

    preserved: list[StoredEvent] = list(events[-3:])
    preserved_ids = {e.event_id for e in preserved}
    for event in events:
        is_error = event.event_type.endswith("Failed")
        is_pending = str(event.payload.get("status", "")).upper() in {"PENDING", "ERROR"}
        if (is_error or is_pending) and event.event_id not in preserved_ids:
            preserved.append(event)
            preserved_ids.add(event.event_id)
    preserved.sort(key=lambda e: e.stream_position)

    pending_work: list[str] = []
    last_completed_action = ""
    has_completion = any(e.event_type == "AgentSessionCompleted" for e in events)
    has_failure = any(e.event_type == "AgentSessionFailed" for e in events)

    for event in events:
        if event.event_type in {"AgentNodeExecuted", "AgentOutputWritten", "AgentSessionCompleted"}:
            last_completed_action = event.event_type
        if event.event_type == "AgentOutputWritten":
            for emitted in event.payload.get("events_written", []) or []:
                et = str(emitted.get("event_type", ""))
                if et in {
                    "DecisionGenerated",
                    "CreditAnalysisCompleted",
                    "FraudScreeningCompleted",
                    "ComplianceCheckCompleted",
                }:
                    pending_work.append(f"Confirm persistence/ack for {et}")
        if event.event_type.endswith("Failed"):
            pending_work.append(f"Recover from {event.event_type}")

    needs_reconciliation = bool(pending_work and not has_completion)
    health = "HEALTHY"
    if has_failure:
        health = "ERROR"
    if needs_reconciliation:
        health = "NEEDS_RECONCILIATION"

    body = [
        f"session_id={session_id}",
        f"agent_id={agent_id}",
        f"event_count={len(events)}",
        f"last_completed_action={last_completed_action or 'none'}",
        f"summary_old_events={summary or 'none'}",
        "verbatim_context:",
    ]
    body.extend(_event_line(event) for event in preserved)
    context_text = _trim_to_budget("\n".join(body), token_budget=token_budget)
    return AgentContext(
        context_text=context_text,
        last_event_position=events[-1].stream_position,
        pending_work=sorted(set(pending_work)),
        session_health_status=health,
    )
