"""Append HumanReviewCompleted + binding when confidence meets the HITL floor (no HumanReviewRequested)."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from aggregates.compliance_record import ComplianceRecordAggregate
from aggregates.loan_application import LoanApplicationAggregate
from aggregates.replay import event_type_from_stored, payload_from_stored
from models.events import (
    ApplicationApproved,
    ApplicationDeclined,
    DomainError,
    HumanReviewCompleted,
)

logger = logging.getLogger(__name__)


def _refer_auto_outcome() -> str:
    v = os.getenv("LEDGER_AUTO_BIND_REFER_AS", "approve").strip().lower()
    return "DECLINE" if v == "decline" else "APPROVE"


async def build_auto_binding_events_after_high_confidence_decision(
    store,
    application_id: str,
    *,
    recommendation: str,
    confidence: float,
    hitl_threshold: float,
) -> list[dict[str, Any]] | None:
    """Return [HumanReviewCompleted, ApplicationApproved|Declined] or None if not applicable / skipped."""
    rec = str(recommendation).upper()
    if confidence < hitl_threshold:
        return None

    reviewer = (
        os.getenv("LEDGER_AUTO_BINDING_REVIEWER_ID", "SYSTEM-HIGH-CONFIDENCE").strip()
        or "SYSTEM-HIGH-CONFIDENCE"
    )
    reviewed_at = datetime.now(UTC)

    stream_id = f"loan-{application_id}"
    loan_events = await store.load_stream(stream_id)
    decision_payload: dict[str, Any] | None = None
    for ev in reversed(loan_events):
        if event_type_from_stored(ev) == "DecisionGenerated":
            decision_payload = payload_from_stored(ev)
            break

    if rec == "REFER":
        final = _refer_auto_outcome()
        rationale = (
            f"Automatic binding: confidence_score={confidence:.2f}>={hitl_threshold}; "
            f"model recommendation=REFER resolved to {final} (no HumanReviewRequested; "
            f"LEDGER_AUTO_BIND_REFER_AS={'decline' if final == 'DECLINE' else 'approve'})."
        )
        return await _human_review_plus_binding(
            store,
            application_id,
            decision_payload=decision_payload,
            reviewer=reviewer,
            reviewed_at=reviewed_at,
            original_recommendation="REFER",
            final_decision=final,
            override=True,
            rationale=rationale,
        )

    if rec not in ("APPROVE", "DECLINE"):
        return None

    rationale = (
        f"Automatic binding: confidence_score={confidence:.2f}>={hitl_threshold}; "
        f"model recommendation={rec}; no HumanReviewRequested (floor met)."
    )
    return await _human_review_plus_binding(
        store,
        application_id,
        decision_payload=decision_payload,
        reviewer=reviewer,
        reviewed_at=reviewed_at,
        original_recommendation=rec,
        final_decision=rec,
        override=False,
        rationale=rationale,
    )


async def _human_review_plus_binding(
    store,
    application_id: str,
    *,
    decision_payload: dict[str, Any] | None,
    reviewer: str,
    reviewed_at: datetime,
    original_recommendation: str,
    final_decision: str,
    override: bool,
    rationale: str,
) -> list[dict[str, Any]] | None:
    fd = str(final_decision).upper()
    if fd == "APPROVE":
        app = await LoanApplicationAggregate.load(store, application_id)
        compliance = await ComplianceRecordAggregate.load(store, application_id)
        try:
            app.assert_may_append_application_approved(compliance, [])
        except DomainError as exc:
            logger.warning(
                "LEDGER_AUTO_BIND: skipped approve (compliance): %s app=%s",
                exc.message,
                application_id,
            )
            return None
        if not decision_payload:
            logger.warning("LEDGER_AUTO_BIND: no DecisionGenerated payload app=%s", application_id)
            return None
        approved_amount_raw = decision_payload.get("approved_amount_usd")
        approved_amount_usd = (
            Decimal(str(approved_amount_raw))
            if approved_amount_raw is not None
            else (
                Decimal(str(app.requested_amount_usd))
                if app.requested_amount_usd is not None
                else None
            )
        )
        if approved_amount_usd is None:
            logger.warning("LEDGER_AUTO_BIND: missing approved_amount_usd app=%s", application_id)
            return None
        conditions = decision_payload.get("conditions") or []
        conditions_list = [str(c) for c in conditions] if isinstance(conditions, list) else []

        human = HumanReviewCompleted(
            application_id=application_id,
            reviewer_id=reviewer,
            override=override,
            original_recommendation=original_recommendation,
            final_decision=fd,
            override_reason=rationale,
            reviewed_at=reviewed_at,
        ).to_store_dict()
        approved = ApplicationApproved(
            application_id=application_id,
            approved_amount_usd=approved_amount_usd,
            interest_rate_pct=9.5,
            term_months=36,
            approved_by=reviewer,
            approved_at=reviewed_at,
            conditions=conditions_list,
        ).to_store_dict()
        return [human, approved]

    human = HumanReviewCompleted(
        application_id=application_id,
        reviewer_id=reviewer,
        override=override,
        original_recommendation=original_recommendation,
        final_decision="DECLINE",
        override_reason=rationale,
        reviewed_at=reviewed_at,
    ).to_store_dict()
    declined = ApplicationDeclined(
        application_id=application_id,
        decline_reasons=[rationale],
        declined_by=reviewer,
        adverse_action_notice_required=True,
        adverse_action_codes=["AUTO_HIGH_CONFIDENCE_DECLINE"],
        declined_at=reviewed_at,
    ).to_store_dict()
    return [human, declined]
