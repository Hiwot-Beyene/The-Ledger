from __future__ import annotations

from datetime import UTC, datetime

import pytest

from event_store import InMemoryEventStore
from integrity.gas_town import reconstruct_agent_context
from gas_town_demo import append_gas_town_crash_demo


@pytest.mark.asyncio
async def test_reconstruct_agent_context_after_simulated_crash() -> None:
    store = InMemoryEventStore()
    stream_id = "agent-credit_analysis-sess-gas-1"
    now = datetime.now(UTC).isoformat()
    await store.append(
        stream_id,
        [
            {
                "event_type": "AgentContextLoaded",
                "event_version": 1,
                "payload": {
                    "session_id": "sess-gas-1",
                    "agent_id": "credit_analysis",
                    "application_id": "app-gas",
                    "context_source": "event_replay",
                    "model_version": "credit-v4",
                    "loaded_at": now,
                },
            },
            {
                "event_type": "AgentSessionStarted",
                "event_version": 1,
                "payload": {
                    "session_id": "sess-gas-1",
                    "agent_type": "credit_analysis",
                    "agent_id": "credit_analysis",
                    "application_id": "app-gas",
                    "model_version": "credit-v4",
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
                    "session_id": "sess-gas-1",
                    "agent_type": "credit_analysis",
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
                    "session_id": "sess-gas-1",
                    "agent_type": "credit_analysis",
                    "application_id": "app-gas",
                    "events_written": [{"event_type": "DecisionGenerated"}],
                    "output_summary": "decision prepared",
                    "written_at": now,
                },
            },
            {
                "event_type": "AgentSessionFailed",
                "event_version": 1,
                "payload": {
                    "session_id": "sess-gas-1",
                    "agent_type": "credit_analysis",
                    "application_id": "app-gas",
                    "error_type": "Crash",
                    "error_message": "worker died",
                    "recoverable": True,
                    "failed_at": now,
                },
            },
        ],
        expected_version=-1,
    )

    reconstructed = await reconstruct_agent_context(store, "credit_analysis", "sess-gas-1")
    assert reconstructed.last_event_position == 5
    assert reconstructed.session_health_status == "NEEDS_RECONCILIATION"
    assert reconstructed.pending_work
    assert "AgentSessionFailed" in reconstructed.context_text


@pytest.mark.asyncio
async def test_append_gas_town_crash_demo_helper_matches_manual_sequence() -> None:
    store = InMemoryEventStore()
    seeded = await append_gas_town_crash_demo(store, application_id="APEX-APP-1", agent_id="credit_analysis")
    assert seeded["events_appended"] == 5
    assert seeded["stream_id"] == f"agent-{seeded['agent_id']}-{seeded['session_id']}"
    reconstructed = await reconstruct_agent_context(store, seeded["agent_id"], seeded["session_id"])
    assert reconstructed.last_event_position == 5
    assert "AgentSessionFailed" in reconstructed.context_text
