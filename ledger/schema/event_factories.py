"""Validated store payloads for common command → event patterns (use from handlers, not ad-hoc dicts)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ledger.schema.events import (
    ApplicationApproved,
    ApplicationSubmitted,
    ComplianceCheckRequested,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    DecisionGenerated,
    DecisionRequested,
)


def store_dict_credit_analysis_requested(
    application_id: str,
    requested_by: str = "system",
    *,
    requested_at: datetime | None = None,
    priority: str = "NORMAL",
) -> dict[str, Any]:
    return CreditAnalysisRequested(
        application_id=application_id,
        requested_at=requested_at or datetime.now(UTC),
        requested_by=requested_by,
        priority=priority,
    ).to_store_dict()


def store_dict_compliance_check_requested(
    application_id: str,
    triggered_by_event_id: str,
    regulation_set_version: str,
    rules_to_evaluate: list[str],
    *,
    requested_at: datetime | None = None,
) -> dict[str, Any]:
    return ComplianceCheckRequested(
        application_id=application_id,
        requested_at=requested_at or datetime.now(UTC),
        triggered_by_event_id=triggered_by_event_id,
        regulation_set_version=regulation_set_version,
        rules_to_evaluate=list(rules_to_evaluate),
    ).to_store_dict()


def store_dict_decision_requested(
    application_id: str,
    triggered_by_event_id: str,
    *,
    all_analyses_complete: bool = True,
    requested_at: datetime | None = None,
) -> dict[str, Any]:
    return DecisionRequested(
        application_id=application_id,
        requested_at=requested_at or datetime.now(UTC),
        all_analyses_complete=all_analyses_complete,
        triggered_by_event_id=triggered_by_event_id,
    ).to_store_dict()


def store_dict_application_submitted(
    application_id: str,
    applicant_id: str,
    requested_amount_usd: Decimal | float | int,
    *,
    submitted_at: datetime | None = None,
) -> dict[str, Any]:
    return ApplicationSubmitted(
        application_id=application_id,
        applicant_id=applicant_id,
        requested_amount_usd=Decimal(str(requested_amount_usd)),
        submitted_at=submitted_at or datetime.now(UTC),
    ).to_store_dict()


def store_dict_credit_analysis_completed_for_loan_stream(
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    confidence_score: float,
    risk_tier: str,
    recommended_limit_usd: Decimal | float | int,
    analysis_duration_ms: int,
    input_data_hash: str,
    *,
    completed_at: datetime | None = None,
) -> dict[str, Any]:
    return CreditAnalysisCompleted(
        application_id=application_id,
        session_id=session_id,
        agent_id=agent_id,
        confidence_score=confidence_score,
        risk_tier=risk_tier,
        recommended_limit_usd=Decimal(str(recommended_limit_usd)),
        model_version=model_version,
        input_data_hash=input_data_hash,
        analysis_duration_ms=analysis_duration_ms,
        completed_at=completed_at or datetime.now(UTC),
    ).to_store_dict()


def store_dict_decision_generated_for_loan_stream(
    application_id: str,
    recommendation: str,
    confidence_score: float,
    contributing_agent_sessions: list[str],
    executive_summary: str,
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    return DecisionGenerated(
        application_id=application_id,
        recommendation=recommendation,
        confidence_score=confidence_score,
        contributing_agent_sessions=contributing_agent_sessions,
        executive_summary=executive_summary,
        generated_at=generated_at or datetime.now(UTC),
    ).to_store_dict()


def store_dict_application_approved(
    application_id: str,
    approved_amount_usd: Decimal | float | int,
    approved_by: str,
    interest_rate_pct: float,
    term_months: int,
    *,
    approved_at: datetime | None = None,
    effective_date: str | None = None,
    conditions: list[str] | None = None,
) -> dict[str, Any]:
    at = approved_at or datetime.now(UTC)
    eff = effective_date if effective_date is not None else at.date().isoformat()
    return ApplicationApproved(
        application_id=application_id,
        approved_amount_usd=Decimal(str(approved_amount_usd)),
        approved_by=approved_by,
        interest_rate_pct=interest_rate_pct,
        term_months=term_months,
        effective_date=eff,
        approved_at=at,
        conditions=conditions or [],
    ).to_store_dict()
