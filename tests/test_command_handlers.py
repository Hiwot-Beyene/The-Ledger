import logging
from types import SimpleNamespace

import pytest

from ledger.domain.commands.handlers import (
    ApplicationApprovedCommand,
    CreditAnalysisCompletedCommand,
    DecisionGeneratedCommand,
    SubmitApplicationCommand,
    handle_application_approved,
    handle_credit_analysis_completed,
    handle_decision_generated,
    handle_submit_application,
    hash_inputs,
)
from ledger.schema.events import DomainError

pytestmark = [pytest.mark.command_handler]


class SpyStore:
    def __init__(self, streams: dict[str, list[SimpleNamespace]]) -> None:
        self.streams = streams
        self.append_calls: list[dict] = []
        self.load_stream_ids: list[str] = []

    async def load_stream(self, stream_id: str) -> list[SimpleNamespace]:
        self.load_stream_ids.append(stream_id)
        return self.streams.get(stream_id, [])

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int:
        self.append_calls.append(
            {
                "stream_id": stream_id,
                "events": events,
                "expected_version": expected_version,
                "correlation_id": correlation_id,
                "causation_id": causation_id,
            }
        )
        return expected_version + len(events)


def _evt(event_type: str, position: int, payload: dict) -> SimpleNamespace:
    return SimpleNamespace(
        event_type=event_type,
        stream_position=position,
        payload=payload,
    )


def _loan_pending_decision_tail(app_id: str) -> list[SimpleNamespace]:
    return [
        _evt("ApplicationSubmitted", 1, {"applicant_id": "C-x", "requested_amount_usd": 1000}),
        _evt("CreditAnalysisRequested", 2, {"application_id": app_id}),
        _evt("CreditAnalysisCompleted", 3, {"application_id": app_id}),
        _evt("ComplianceCheckRequested", 4, {"rules_to_evaluate": ["r1"]}),
        _evt("DecisionRequested", 5, {"application_id": app_id}),
    ]


@pytest.mark.asyncio
async def test_handle_submit_application_appends_with_replayed_version_and_trace_ids():
    store = SpyStore(streams={})
    cmd = SubmitApplicationCommand(
        application_id="A-1",
        applicant_id="C-1",
        requested_amount_usd=50000,
        correlation_id="corr-1",
        causation_id="cause-1",
    )

    await handle_submit_application(cmd, store)

    assert len(store.append_calls) == 1
    call = store.append_calls[0]
    assert call["stream_id"] == "loan-A-1"
    assert call["expected_version"] == -1
    assert call["correlation_id"] == "corr-1"
    assert call["causation_id"] == "cause-1"
    assert call["events"][0]["event_type"] == "ApplicationSubmitted"
    assert call["events"][0]["payload"]["application_id"] == "A-1"
    assert store.load_stream_ids == ["loan-A-1"]


@pytest.mark.asyncio
async def test_handle_submit_application_second_submit_raises_and_no_append():
    streams = {
        "loan-A-dup": [
            _evt("ApplicationSubmitted", 1, {"applicant_id": "C", "requested_amount_usd": 1}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = SubmitApplicationCommand(application_id="A-dup", applicant_id="C", requested_amount_usd=99)

    with pytest.raises(DomainError) as exc:
        await handle_submit_application(cmd, store)

    assert exc.value.code == "application_already_exists"
    assert store.append_calls == []


@pytest.mark.asyncio
async def test_handle_credit_analysis_completed_uses_guards_and_occ_append():
    streams = {
        "loan-A-2": [
            _evt("ApplicationSubmitted", 1, {"applicant_id": "C-2", "requested_amount_usd": 1000}),
            _evt("CreditAnalysisRequested", 2, {"application_id": "A-2"}),
        ],
        "agent-credit-s-1": [
            _evt("AgentContextLoaded", 1, {"model_version": "v1"}),
            _evt("AgentSessionStarted", 2, {"model_version": "v1"}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = CreditAnalysisCompletedCommand(
        application_id="A-2",
        agent_id="credit",
        session_id="s-1",
        model_version="v1",
        confidence_score=0.91,
        risk_tier="LOW",
        recommended_limit_usd=75000,
        duration_ms=1234,
        input_data={"x": 1},
        correlation_id="corr-2",
        causation_id="cause-2",
    )

    await handle_credit_analysis_completed(cmd, store)

    call = store.append_calls[0]
    assert call["stream_id"] == "loan-A-2"
    assert call["expected_version"] == 2
    assert call["correlation_id"] == "corr-2"
    assert call["causation_id"] == "cause-2"
    payload = call["events"][0]["payload"]
    assert payload["input_data_hash"] == hash_inputs({"x": 1})
    assert payload["model_version"] == "v1"
    assert store.load_stream_ids == ["loan-A-2", "agent-credit-s-1"]


@pytest.mark.asyncio
async def test_handle_credit_analysis_completed_wrong_app_state_no_append():
    streams = {
        "loan-A-bad": [_evt("ApplicationSubmitted", 1, {"applicant_id": "C", "requested_amount_usd": 1})],
        "agent-credit-sx": [
            _evt("AgentContextLoaded", 1, {"model_version": "v1"}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = CreditAnalysisCompletedCommand(
        application_id="A-bad",
        agent_id="credit",
        session_id="sx",
        model_version="v1",
        confidence_score=0.9,
        risk_tier="LOW",
        recommended_limit_usd=1,
        duration_ms=1,
        input_data={},
    )

    with pytest.raises(DomainError) as exc:
        await handle_credit_analysis_completed(cmd, store)

    assert exc.value.code == "application_not_awaiting_analysis"
    assert store.append_calls == []


@pytest.mark.asyncio
async def test_handle_credit_analysis_completed_model_mismatch_no_append():
    streams = {
        "loan-A-mv": [
            _evt("ApplicationSubmitted", 1, {"applicant_id": "C", "requested_amount_usd": 1}),
            _evt("CreditAnalysisRequested", 2, {"application_id": "A-mv"}),
        ],
        "agent-credit-smv": [
            _evt("AgentContextLoaded", 1, {"model_version": "v-session"}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = CreditAnalysisCompletedCommand(
        application_id="A-mv",
        agent_id="credit",
        session_id="smv",
        model_version="v-cmd",
        confidence_score=0.9,
        risk_tier="LOW",
        recommended_limit_usd=1,
        duration_ms=1,
        input_data={},
    )

    with pytest.raises(DomainError) as exc:
        await handle_credit_analysis_completed(cmd, store)

    assert exc.value.code == "model_version_mismatch"
    assert store.append_calls == []


@pytest.mark.asyncio
async def test_handle_credit_analysis_completed_agent_gas_town_violation_no_append():
    streams = {
        "loan-A-gas": [
            _evt("ApplicationSubmitted", 1, {"applicant_id": "C", "requested_amount_usd": 1}),
            _evt("CreditAnalysisRequested", 2, {"application_id": "A-gas"}),
        ],
        "agent-credit-sgas": [
            _evt("AgentSessionStarted", 1, {"model_version": "v1"}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = CreditAnalysisCompletedCommand(
        application_id="A-gas",
        agent_id="credit",
        session_id="sgas",
        model_version="v1",
        confidence_score=0.9,
        risk_tier="LOW",
        recommended_limit_usd=1,
        duration_ms=1,
        input_data={},
    )

    with pytest.raises(DomainError) as exc:
        await handle_credit_analysis_completed(cmd, store)

    assert exc.value.code == "agent_context_required"
    assert store.append_calls == []


@pytest.mark.asyncio
async def test_handle_decision_generated_enforces_confidence_floor_and_causal_chain():
    app_id = "A-3"
    streams = {
        f"loan-{app_id}": [
            *_loan_pending_decision_tail(app_id),
        ],
        "agent-credit-s-3": [
            _evt("AgentContextLoaded", 1, {"model_version": "v2"}),
            _evt("DecisionGenerated", 2, {"application_id": app_id}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = DecisionGeneratedCommand(
        application_id=app_id,
        recommendation="APPROVE",
        confidence_score=0.4,
        contributing_agent_sessions=["agent-credit-s-3"],
        summary="summary",
        correlation_id="corr-3",
        causation_id="cause-3",
    )

    await handle_decision_generated(cmd, store)

    call = store.append_calls[0]
    assert call["stream_id"] == f"loan-{app_id}"
    assert call["expected_version"] == 5
    assert call["correlation_id"] == "corr-3"
    assert call["causation_id"] == "cause-3"
    assert call["events"][0]["payload"]["recommendation"] == "REFER"
    assert store.load_stream_ids == [f"loan-{app_id}", "agent-credit-s-3"]


@pytest.mark.asyncio
async def test_handle_decision_generated_loads_each_contributing_session_in_order():
    app_id = "A-multi"
    streams = {
        f"loan-{app_id}": [*_loan_pending_decision_tail(app_id)],
        "agent-credit-s5": [
            _evt("AgentContextLoaded", 1, {"model_version": "m"}),
            _evt("DecisionGenerated", 2, {"application_id": app_id}),
        ],
        "agent-fraud-s5": [
            _evt("AgentContextLoaded", 1, {"model_version": "m"}),
            _evt("DecisionGenerated", 2, {"application_id": app_id}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = DecisionGeneratedCommand(
        application_id=app_id,
        recommendation="DECLINE",
        confidence_score=0.95,
        contributing_agent_sessions=["agent-credit-s5", "agent-fraud-s5"],
        summary="s",
    )

    await handle_decision_generated(cmd, store)

    assert store.load_stream_ids == [
        f"loan-{app_id}",
        "agent-credit-s5",
        "agent-fraud-s5",
    ]
    assert len(store.append_calls) == 1


@pytest.mark.asyncio
async def test_handle_decision_generated_invalid_stream_id_before_append():
    app_id = "A-badid"
    store = SpyStore(streams={f"loan-{app_id}": [*_loan_pending_decision_tail(app_id)]})
    cmd = DecisionGeneratedCommand(
        application_id=app_id,
        recommendation="REFER",
        confidence_score=0.9,
        contributing_agent_sessions=["not-agent-x"],
        summary="s",
    )

    with pytest.raises(DomainError) as exc:
        await handle_decision_generated(cmd, store)

    assert exc.value.code == "invalid_agent_stream_id"
    assert store.append_calls == []


@pytest.mark.asyncio
async def test_handle_decision_generated_causal_chain_failure_no_append():
    app_id = "A-nocausal"
    streams = {
        f"loan-{app_id}": [*_loan_pending_decision_tail(app_id)],
        "agent-credit-snc": [
            _evt("AgentContextLoaded", 1, {"model_version": "m"}),
            _evt("DecisionGenerated", 2, {"application_id": "OTHER"}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = DecisionGeneratedCommand(
        application_id=app_id,
        recommendation="REFER",
        confidence_score=0.9,
        contributing_agent_sessions=["agent-credit-snc"],
        summary="s",
    )

    with pytest.raises(DomainError) as exc:
        await handle_decision_generated(cmd, store)

    assert exc.value.code == "invalid_causal_chain"
    assert store.append_calls == []


@pytest.mark.asyncio
async def test_handle_application_approved_enforces_compliance_dependency():
    streams = {
        "loan-A-4": [
            _evt("ApplicationSubmitted", 1, {"applicant_id": "C-4", "requested_amount_usd": 1000}),
            _evt("CreditAnalysisRequested", 2, {"application_id": "A-4"}),
        ],
        "compliance-A-4": [
            _evt("ComplianceCheckRequested", 1, {"rules_to_evaluate": ["r1", "r2"]}),
            _evt("ComplianceRulePassed", 2, {"rule_id": "r1"}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = ApplicationApprovedCommand(
        application_id="A-4",
        approved_amount_usd=120000,
        approved_by="underwriter",
        interest_rate_pct=8.2,
        term_months=24,
        required_checks=["r1", "r2"],
    )

    with pytest.raises(DomainError) as exc:
        await handle_application_approved(cmd, store)

    assert exc.value.code == "compliance_dependency_failed"
    assert len(store.append_calls) == 0
    assert "compliance-A-4" in store.load_stream_ids


@pytest.mark.asyncio
async def test_handle_application_approved_happy_path_occ_and_metadata():
    app_id = "A-ok"
    streams = {
        f"loan-{app_id}": [
            *_loan_pending_decision_tail(app_id),
            _evt(
                "DecisionGenerated",
                6,
                {"recommendation": "APPROVE", "confidence_score": 0.92},
            ),
        ],
        f"compliance-{app_id}": [
            _evt("ComplianceCheckRequested", 1, {"rules_to_evaluate": ["r1", "r2"]}),
            _evt("ComplianceRulePassed", 2, {"rule_id": "r1"}),
            _evt("ComplianceRulePassed", 3, {"rule_id": "r2"}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = ApplicationApprovedCommand(
        application_id=app_id,
        approved_amount_usd=50000,
        approved_by="u1",
        interest_rate_pct=7.0,
        term_months=36,
        required_checks=["r1", "r2"],
        correlation_id="c-ok",
        causation_id="z-ok",
    )

    await handle_application_approved(cmd, store)

    assert store.load_stream_ids[0] == f"loan-{app_id}"
    assert store.load_stream_ids[1] == f"compliance-{app_id}"
    call = store.append_calls[0]
    assert call["expected_version"] == 6
    assert call["stream_id"] == f"loan-{app_id}"
    assert call["correlation_id"] == "c-ok"
    assert call["causation_id"] == "z-ok"
    assert call["events"][0]["event_type"] == "ApplicationApproved"
    assert call["events"][0]["payload"]["approved_by"] == "u1"


@pytest.mark.asyncio
async def test_observability_success_logs_start_and_ok(caplog):
    caplog.set_level(logging.INFO, logger="ledger.commands")
    store = SpyStore(streams={})
    cmd = SubmitApplicationCommand(
        application_id="LOG-1",
        applicant_id="C",
        requested_amount_usd=1,
        correlation_id="cr",
        causation_id="ca",
    )

    await handle_submit_application(cmd, store)

    joined = caplog.text
    assert "handler_start handle_submit_application" in joined
    assert "correlation_id='cr'" in joined
    assert "application_id='LOG-1'" in joined
    assert "handler_ok handle_submit_application" in joined
    assert "handler_failed" not in joined


@pytest.mark.asyncio
async def test_observability_credit_handler_failure_logs(caplog):
    caplog.set_level(logging.INFO, logger="ledger.commands")
    streams = {
        "loan-A-obs": [
            _evt("ApplicationSubmitted", 1, {"applicant_id": "C", "requested_amount_usd": 1}),
            _evt("CreditAnalysisRequested", 2, {"application_id": "A-obs"}),
        ],
        "agent-credit-sobs": [
            _evt("AgentContextLoaded", 1, {"model_version": "v-a"}),
        ],
    }
    store = SpyStore(streams=streams)
    cmd = CreditAnalysisCompletedCommand(
        application_id="A-obs",
        agent_id="credit",
        session_id="sobs",
        model_version="v-b",
        confidence_score=0.9,
        risk_tier="LOW",
        recommended_limit_usd=1,
        duration_ms=1,
        input_data={},
    )

    with pytest.raises(DomainError):
        await handle_credit_analysis_completed(cmd, store)

    text = caplog.text
    assert "handler_start handle_credit_analysis_completed" in text
    assert "handler_failed handle_credit_analysis_completed" in text
    assert "handler_ok handle_credit_analysis_completed" not in text


@pytest.mark.asyncio
async def test_observability_failure_logs_start_and_failed_not_ok(caplog):
    caplog.set_level(logging.INFO, logger="ledger.commands")
    streams = {
        "loan-X": [_evt("ApplicationSubmitted", 1, {"applicant_id": "C", "requested_amount_usd": 1})],
    }
    store = SpyStore(streams=streams)
    cmd = SubmitApplicationCommand(application_id="X", applicant_id="C", requested_amount_usd=2)

    with pytest.raises(DomainError):
        await handle_submit_application(cmd, store)

    joined = caplog.text
    assert "handler_start handle_submit_application" in joined
    assert "handler_failed handle_submit_application" in joined
    assert "handler_ok handle_submit_application" not in joined
