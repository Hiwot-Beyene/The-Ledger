"""Unit tests: LoanApplicationAggregate replay, VALID_TRANSITIONS, and guards."""

from types import SimpleNamespace

import pytest

from ledger.domain.aggregates.loan_application import (
    ApplicationState,
    LoanApplicationAggregate,
    VALID_TRANSITIONS,
)
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate
from ledger.schema.events import DomainError


def _evt(event_type: str, stream_position: int, payload: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        event_type=event_type,
        stream_position=stream_position,
        payload=payload or {},
    )


def _replay(application_id: str, *events: SimpleNamespace) -> LoanApplicationAggregate:
    agg = LoanApplicationAggregate(application_id=application_id)
    for e in events:
        agg._apply(e)
    return agg


@pytest.mark.asyncio
async def test_valid_transitions_map_is_symmetric_with_documented_machine():
    assert ApplicationState.SUBMITTED in VALID_TRANSITIONS
    assert ApplicationState.AWAITING_ANALYSIS in VALID_TRANSITIONS[ApplicationState.SUBMITTED]
    assert ApplicationState.FINAL_APPROVED not in VALID_TRANSITIONS.get(ApplicationState.SUBMITTED, set())


def test_replay_happy_path_state_and_version():
    agg = _replay(
        "APP-1",
        _evt("ApplicationSubmitted", 1, {"applicant_id": "C1", "requested_amount_usd": 1000}),
        _evt("CreditAnalysisRequested", 2, {"application_id": "APP-1"}),
        _evt("CreditAnalysisCompleted", 3, {"application_id": "APP-1"}),
        _evt("ComplianceCheckRequested", 4, {"rules_to_evaluate": ["r1"]}),
        _evt("DecisionRequested", 5, {"application_id": "APP-1"}),
    )
    assert agg.state == ApplicationState.PENDING_DECISION
    assert agg.version == 5
    assert agg.required_compliance_checks == {"r1"}
    assert agg.credit_analysis_completed is True


def test_replay_invalid_sequence_submitted_then_decision_requested_raises():
    with pytest.raises(DomainError) as exc:
        _replay(
            "APP-2",
            _evt("ApplicationSubmitted", 1, {"applicant_id": "C1", "requested_amount_usd": 1}),
            _evt("DecisionRequested", 2, {"application_id": "APP-2"}),
        )
    assert exc.value.code == "invalid_transition"


def test_assert_new_application_stream_ok_and_raises():
    fresh = LoanApplicationAggregate(application_id="X")
    fresh.assert_new_application_stream()

    bad = _evt("ApplicationSubmitted", 1, {"applicant_id": "a", "requested_amount_usd": 1})
    agg = LoanApplicationAggregate(application_id="Y")
    agg._apply(bad)
    with pytest.raises(DomainError) as exc:
        agg.assert_new_application_stream()
    assert exc.value.code == "application_already_exists"


def test_assert_awaiting_credit_analysis_states():
    agg = LoanApplicationAggregate(application_id="Z")
    with pytest.raises(DomainError) as exc:
        agg.assert_awaiting_credit_analysis()
    assert exc.value.code == "application_not_awaiting_analysis"

    agg2 = _replay(
        "Z2",
        _evt("ApplicationSubmitted", 1, {"applicant_id": "a", "requested_amount_usd": 1}),
        _evt("CreditAnalysisRequested", 2, {}),
    )
    agg2.assert_awaiting_credit_analysis()

    agg3 = _replay(
        "Z3",
        _evt("ApplicationSubmitted", 1, {"applicant_id": "a", "requested_amount_usd": 1}),
        _evt("CreditAnalysisRequested", 2, {}),
        _evt("CreditAnalysisCompleted", 3, {}),
    )
    with pytest.raises(DomainError) as exc:
        agg3.assert_awaiting_credit_analysis()
    assert exc.value.code == "credit_analysis_locked"

    agg4 = _replay(
        "Z4",
        _evt("ApplicationSubmitted", 1, {"applicant_id": "a", "requested_amount_usd": 1}),
        _evt("CreditAnalysisRequested", 2, {}),
        _evt("CreditAnalysisCompleted", 3, {}),
        _evt("HumanReviewOverride", 4, {}),
    )
    agg4.assert_awaiting_credit_analysis()


def test_assert_decision_recommendation_valid_confidence_floor():
    agg = LoanApplicationAggregate(application_id="D1")
    agg.state = ApplicationState.PENDING_DECISION
    with pytest.raises(DomainError) as exc:
        agg.assert_decision_recommendation_valid("APPROVE", 0.4)
    assert exc.value.code == "confidence_floor_violation"

    agg.assert_decision_recommendation_valid("REFER", 0.4)


def test_effective_decision_recommendation_normalizes():
    agg = LoanApplicationAggregate(application_id="D2")
    agg.state = ApplicationState.PENDING_DECISION
    assert agg.effective_decision_recommendation("approve", 0.9) == "APPROVE"
    assert agg.effective_decision_recommendation("APPROVE", 0.4) == "REFER"


def test_assert_compliance_dependency():
    agg = LoanApplicationAggregate(application_id="C1")
    agg.required_compliance_checks = {"a", "b"}
    with pytest.raises(DomainError) as exc:
        agg.assert_compliance_dependency({"a"})
    assert exc.value.code == "compliance_dependency_failed"
    assert "b" in exc.value.message

    agg.assert_compliance_dependency({"a", "b", "c"})


def test_assert_may_append_application_approved_merges_required():
    agg = LoanApplicationAggregate(application_id="C2")
    agg.required_compliance_checks = {"x"}
    comp = ComplianceRecordAggregate(application_id="C2")
    comp.required_checks = {"y"}
    comp.passed_checks = {"x", "y"}

    agg.assert_may_append_application_approved(comp, [])
    assert agg.required_compliance_checks == {"x"}


def test_decision_generated_low_confidence_emits_refer_transition_stays_pending():
    agg = _replay(
        "DG1",
        _evt("ApplicationSubmitted", 1, {"applicant_id": "a", "requested_amount_usd": 1}),
        _evt("CreditAnalysisRequested", 2, {}),
        _evt("CreditAnalysisCompleted", 3, {}),
        _evt("ComplianceCheckRequested", 4, {"rules_to_evaluate": []}),
        _evt("DecisionRequested", 5, {}),
        _evt(
            "DecisionGenerated",
            6,
            {"recommendation": "REFER", "confidence_score": 0.4},
        ),
    )
    assert agg.state == ApplicationState.PENDING_DECISION
    assert agg.last_decision_recommendation == "REFER"


def test_decision_generated_invalid_low_confidence_approve_raises_on_apply():
    base = [
        _evt("ApplicationSubmitted", 1, {"applicant_id": "a", "requested_amount_usd": 1}),
        _evt("CreditAnalysisRequested", 2, {}),
        _evt("CreditAnalysisCompleted", 3, {}),
        _evt("ComplianceCheckRequested", 4, {"rules_to_evaluate": []}),
        _evt("DecisionRequested", 5, {}),
    ]
    with pytest.raises(DomainError) as exc:
        _replay(
            "DG2",
            *base,
            _evt("DecisionGenerated", 6, {"recommendation": "APPROVE", "confidence_score": 0.4}),
        )
    assert exc.value.code == "confidence_floor_violation"
