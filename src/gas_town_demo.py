"""Deterministic Gas Town crash-recovery demo: append agent session events, then reconstruct from the store."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from event_store import EventStore


def build_gas_town_crash_demo_events(
    *,
    session_id: str,
    agent_id: str,
    application_id: str,
    model_version: str = "credit-v4",
) -> list[dict[str, Any]]:
    now = datetime.now(UTC).isoformat()
    return [
        {
            "event_type": "AgentContextLoaded",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_id": agent_id,
                "application_id": application_id,
                "context_source": "event_replay",
                "model_version": model_version,
                "loaded_at": now,
            },
        },
        {
            "event_type": "AgentSessionStarted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_id,
                "agent_id": agent_id,
                "application_id": application_id,
                "model_version": model_version,
                "langgraph_graph_version": "g1",
                "context_source": "event_replay",
                "context_token_count": 2000,
                "started_at": now,
            },
        },
        {
            "event_type": "AgentNodeExecuted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_id,
                "node_name": "extract_features",
                "node_sequence": 1,
                "input_keys": ["facts"],
                "output_keys": ["features"],
                "llm_called": False,
                "duration_ms": 10,
                "executed_at": now,
            },
        },
        {
            "event_type": "AgentOutputWritten",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_id,
                "application_id": application_id,
                "events_written": [{"event_type": "DecisionGenerated"}],
                "output_summary": "decision prepared",
                "written_at": now,
            },
        },
        {
            "event_type": "AgentSessionFailed",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_id,
                "application_id": application_id,
                "error_type": "Crash",
                "error_message": "worker died (simulated — events already persisted)",
                "recoverable": True,
                "failed_at": now,
            },
        },
    ]


async def append_gas_town_crash_demo(
    store: EventStore,
    *,
    application_id: str,
    agent_id: str = "credit_analysis",
) -> dict[str, Any]:
    """Append five agent-session events to ``agent-{agent_id}-{session_id}`` (new session each call)."""
    sid = f"sess-gas-{uuid4().hex[:10]}"
    stream_id = f"agent-{agent_id}-{sid}"
    events = build_gas_town_crash_demo_events(
        session_id=sid,
        agent_id=agent_id,
        application_id=application_id.strip() or "APEX-GAS-DEMO",
    )
    await store.append(stream_id, events, expected_version=-1)
    return {
        "stream_id": stream_id,
        "session_id": sid,
        "agent_id": agent_id,
        "application_id": application_id.strip() or "APEX-GAS-DEMO",
        "events_appended": len(events),
        "last_stream_position": len(events),
        "narrative": [
            "AgentContextLoaded — hydration from replay (Gas Town rule).",
            "AgentSessionStarted — in-memory runner would hold graph state here.",
            "AgentNodeExecuted — mid-run progress.",
            "AgentOutputWritten — domain events emitted; reconciliation may be pending.",
            "AgentSessionFailed — simulates kill/crash after persistence (recoverable).",
        ],
    }
