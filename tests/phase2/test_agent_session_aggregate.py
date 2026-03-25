"""Unit tests: AgentSessionAggregate replay, Gas Town guards, parse_contributing_stream_id."""

from types import SimpleNamespace

import pytest

from aggregates.agent_session import AgentSessionAggregate
from models.events import DomainError


def _evt(event_type: str, stream_position: int, payload: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        event_type=event_type,
        stream_position=stream_position,
        payload=payload or {},
    )


def _replay(agent_id: str, session_id: str, *events: SimpleNamespace) -> AgentSessionAggregate:
    agg = AgentSessionAggregate(agent_id=agent_id, session_id=session_id)
    for e in events:
        agg._apply(e)
    return agg


def test_parse_contributing_stream_id_ok():
    a, s = AgentSessionAggregate.parse_contributing_stream_id(
        "agent-credit-sess-1",
        application_id="APP",
        loan_application_state="PendingDecision",
    )
    assert a == "credit" and s == "sess-1"


def test_parse_contributing_stream_id_nested_session_id():
    a, s = AgentSessionAggregate.parse_contributing_stream_id(
        "agent-fraud_detection-uuid-here",
        application_id="APP",
        loan_application_state="X",
    )
    assert a == "fraud_detection" and s == "uuid-here"


def test_parse_contributing_stream_id_invalid():
    with pytest.raises(DomainError) as exc:
        AgentSessionAggregate.parse_contributing_stream_id(
            "loan-not-agent",
            application_id="APP",
            loan_application_state="S",
        )
    assert exc.value.code == "invalid_agent_stream_id"


def test_gas_town_context_first_then_decision_tracks_application():
    agg = _replay(
        "credit",
        "s1",
        _evt("AgentContextLoaded", 1, {"model_version": "v1"}),
        _evt("AgentSessionStarted", 2, {"model_version": "v1"}),
        _evt("DecisionGenerated", 3, {"application_id": "APEX-1"}),
    )
    assert agg.context_loaded and agg.context_declared_first
    assert agg.model_version == "v1"
    assert "APEX-1" in agg.decision_application_ids
    agg.assert_context_loaded()
    agg.assert_session_processed_application("APEX-1")


def test_session_started_first_violates_gas_town_guard():
    agg = _replay(
        "credit",
        "s2",
        _evt("AgentSessionStarted", 1, {"model_version": "v1"}),
        _evt("DecisionGenerated", 2, {"application_id": "A"}),
    )
    with pytest.raises(DomainError) as exc:
        agg.assert_context_loaded()
    assert exc.value.code == "agent_context_required"


def test_assert_model_version_mismatch():
    agg = _replay(
        "credit",
        "s3",
        _evt("AgentContextLoaded", 1, {"model_version": "v1"}),
    )
    with pytest.raises(DomainError) as exc:
        agg.assert_model_version_current("v2")
    assert exc.value.code == "model_version_mismatch"


def test_assert_session_processed_application_fails():
    agg = _replay(
        "credit",
        "s4",
        _evt("AgentContextLoaded", 1, {"model_version": "m"}),
        _evt("AgentSessionStarted", 2, {}),
    )
    with pytest.raises(DomainError) as exc:
        agg.assert_session_processed_application("unknown-app")
    assert exc.value.code == "invalid_causal_chain"


def test_agent_output_written_tracks_application_when_embedded_event_type_matches():
    agg = _replay(
        "credit",
        "s5",
        _evt("AgentContextLoaded", 1, {"model_version": "m"}),
        _evt(
            "AgentOutputWritten",
            2,
            {
                "application_id": "APP-9",
                "events_written": [{"event_type": "CreditAnalysisCompleted"}],
            },
        ),
    )
    agg.assert_session_processed_application("APP-9")
