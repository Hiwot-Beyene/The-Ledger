"""Pydantic domain events + exception diagnostics aligned with aggregate invariants."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from ledger.schema.events import (
    CreditAnalysisCompleted,
    DecisionGenerated,
    DecisionRecommendation,
    DomainError,
    HumanReviewCompleted,
    OptimisticConcurrencyError,
)


def test_decision_generated_coerces_lowercase_recommendation():
    t = datetime.now(UTC)
    ev = DecisionGenerated(
        application_id="A",
        recommendation="approve",
        executive_summary="x",
        generated_at=t,
        confidence_score=0.9,
    )
    assert ev.recommendation is DecisionRecommendation.APPROVE


def test_decision_generated_rejects_invalid_recommendation():
    t = datetime.now(UTC)
    with pytest.raises(ValidationError):
        DecisionGenerated(
            application_id="A",
            recommendation="MAYBE",
            executive_summary="x",
            generated_at=t,
            confidence_score=0.9,
        )


def test_decision_generated_rejects_confidence_out_of_range():
    t = datetime.now(UTC)
    with pytest.raises(ValidationError):
        DecisionGenerated(
            application_id="A",
            recommendation="REFER",
            executive_summary="x",
            generated_at=t,
            confidence_score=1.01,
        )


def test_credit_analysis_completed_risk_tier_and_confidence():
    t = datetime.now(UTC)
    ev = CreditAnalysisCompleted(
        application_id="A",
        completed_at=t,
        risk_tier="low",
        confidence_score=0.5,
    )
    assert ev.risk_tier == "LOW"
    with pytest.raises(ValidationError):
        CreditAnalysisCompleted(
            application_id="A",
            completed_at=t,
            risk_tier="EXTREME",
            confidence_score=0.5,
        )


def test_human_review_completed_coerces_decisions():
    t = datetime.now(UTC)
    ev = HumanReviewCompleted(
        application_id="A",
        reviewer_id="r1",
        override=True,
        original_recommendation="decline",
        final_decision="approve",
        reviewed_at=t,
    )
    assert ev.final_decision is DecisionRecommendation.APPROVE


def test_domain_error_to_diagnostic_dict():
    e = DomainError(
        code="compliance_dependency_failed",
        message="missing",
        aggregate_id="APP-1",
        current_state="PendingDecision",
        rule="compliance_dependency",
    )
    d = e.to_diagnostic_dict()
    assert d["kind"] == "DomainError"
    assert d["code"] == "compliance_dependency_failed"
    assert d["aggregate_id"] == "APP-1"
    assert d["rule"] == "compliance_dependency"


def test_optimistic_concurrency_to_diagnostic_dict():
    e = OptimisticConcurrencyError("loan-X", 3, 5)
    d = e.to_diagnostic_dict()
    assert d["kind"] == "OptimisticConcurrencyError"
    assert d["stream_id"] == "loan-X"
    assert d["expected_version"] == 3
    assert d["actual_version"] == 5
    assert d["suggested_action"] == "reload_stream_and_retry"


def test_application_submitted_rejects_non_positive_amount():
    from ledger.schema.events import ApplicationSubmitted

    t = datetime.now(UTC)
    with pytest.raises(ValidationError):
        ApplicationSubmitted(
            application_id="A",
            applicant_id="C",
            requested_amount_usd=Decimal("0"),
            submitted_at=t,
        )
