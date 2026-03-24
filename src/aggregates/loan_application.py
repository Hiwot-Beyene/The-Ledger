from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from aggregates.compliance_record import ComplianceRecordAggregate
from aggregates.replay import (
    advance_version_from_stored,
    event_type_from_stored,
    optional_float,
    optional_str,
    payload_from_stored,
    rules_evaluate_ids,
)
from models.events import DomainError, StoredEvent


def _decision_confidence_and_recommendation(payload: dict[str, Any]) -> tuple[float, str]:
    confidence = float(payload.get("confidence_score", payload.get("confidence", 0.0)))
    recommendation = str(payload.get("recommendation", "REFER")).upper()
    return confidence, recommendation


class ApplicationState(str, Enum):
    SUBMITTED = "Submitted"
    AWAITING_ANALYSIS = "AwaitingAnalysis"
    ANALYSIS_COMPLETE = "AnalysisComplete"
    COMPLIANCE_REVIEW = "ComplianceReview"
    PENDING_DECISION = "PendingDecision"
    APPROVED_PENDING_HUMAN = "ApprovedPendingHuman"
    DECLINED_PENDING_HUMAN = "DeclinedPendingHuman"
    FINAL_APPROVED = "FinalApproved"
    FINAL_DECLINED = "FinalDeclined"


VALID_TRANSITIONS: dict[ApplicationState, set[ApplicationState]] = {
    ApplicationState.SUBMITTED: {ApplicationState.AWAITING_ANALYSIS},
    ApplicationState.AWAITING_ANALYSIS: {ApplicationState.ANALYSIS_COMPLETE},
    ApplicationState.ANALYSIS_COMPLETE: {
        ApplicationState.ANALYSIS_COMPLETE,
        ApplicationState.COMPLIANCE_REVIEW,
    },
    ApplicationState.COMPLIANCE_REVIEW: {ApplicationState.PENDING_DECISION},
    ApplicationState.PENDING_DECISION: {
        ApplicationState.PENDING_DECISION,
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
        ApplicationState.FINAL_APPROVED,
        ApplicationState.FINAL_DECLINED,
    },
    ApplicationState.APPROVED_PENDING_HUMAN: {
        ApplicationState.FINAL_APPROVED,
        ApplicationState.FINAL_DECLINED,
    },
    ApplicationState.DECLINED_PENDING_HUMAN: {
        ApplicationState.FINAL_APPROVED,
        ApplicationState.FINAL_DECLINED,
    },
}

STATE_RULE = "application_state_machine"
CONFIDENCE_RULE = "confidence_floor"
MODEL_VERSION_RULE = "model_version_locking"
COMPLIANCE_RULE = "compliance_dependency"
APPLICATION_UNIQUENESS_RULE = "application_uniqueness"

ERROR_MESSAGES = {
    "invalid_initial_transition": "Invalid initial transition {from_state} -> {to_state}",
    "invalid_transition": "Invalid transition {from_state} -> {to_state}",
    "credit_analysis_locked": "CreditAnalysisCompleted already exists and is not overridden",
    "application_not_awaiting_analysis": "Application is not awaiting credit analysis",
    "confidence_floor_violation": "confidence_score < 0.6 must produce recommendation REFER",
    "compliance_dependency_failed": "Missing required compliance checks: {missing_checks}",
    "application_already_exists": "Application {application_id} already exists",
}


class EventStoreLike(Protocol):
    async def load_stream(self, stream_id: str) -> list[StoredEvent]: ...


@dataclass(slots=True)
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState | None = None
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    approved_amount_usd: float | None = None
    version: int = -1
    required_compliance_checks: set[str] = field(default_factory=set)
    passed_compliance_checks: set[str] = field(default_factory=set)
    credit_analysis_completed: bool = False
    credit_analysis_overridden: bool = False
    last_decision_recommendation: str | None = None

    @classmethod
    async def load(cls, store: EventStoreLike, application_id: str) -> "LoanApplicationAggregate":
        events = await store.load_stream(f"loan-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent | dict[str, Any]) -> None:
        event_type = event_type_from_stored(event)
        handler = getattr(self, f"_on_{event_type}", None)
        if handler is not None:
            handler(event)
        self.version = advance_version_from_stored(event, self.version)

    def _raise_domain_error(self, code: str, rule: str, **message_values: Any) -> None:
        current_state = self.state.value if self.state else "NONE"
        message = ERROR_MESSAGES[code].format(**message_values)
        raise DomainError(
            code=code,
            message=message,
            aggregate_id=self.application_id,
            current_state=current_state,
            rule=rule,
        )

    def _transition(self, target: ApplicationState, rule: str) -> None:
        if self.state is None:
            if target == ApplicationState.SUBMITTED:
                self.state = target
                return
            self._raise_domain_error(
                "invalid_initial_transition",
                rule,
                from_state="NONE",
                to_state=target,
            )
        allowed = VALID_TRANSITIONS.get(self.state, set())
        if target not in allowed:
            self._raise_domain_error(
                "invalid_transition",
                rule,
                from_state=self.state,
                to_state=target,
            )
        self.state = target

    def _on_ApplicationSubmitted(self, event: StoredEvent | dict[str, Any]) -> None:
        payload = payload_from_stored(event)
        self._transition(ApplicationState.SUBMITTED, STATE_RULE)
        self.applicant_id = optional_str(payload, "applicant_id")
        self.requested_amount_usd = optional_float(payload, "requested_amount_usd")

    def _on_DocumentUploadRequested(self, event: StoredEvent | dict[str, Any]) -> None:
        return

    def _on_DocumentUploaded(self, event: StoredEvent | dict[str, Any]) -> None:
        return

    def _on_PackageReadyForAnalysis(self, event: StoredEvent | dict[str, Any]) -> None:
        return

    def _on_CreditAnalysisRequested(self, event: StoredEvent | dict[str, Any]) -> None:
        self._transition(ApplicationState.AWAITING_ANALYSIS, STATE_RULE)

    def _on_CreditAnalysisCompleted(self, event: StoredEvent | dict[str, Any]) -> None:
        self._transition(ApplicationState.ANALYSIS_COMPLETE, STATE_RULE)
        self.credit_analysis_completed = True

    def _on_FraudScreeningRequested(self, event: StoredEvent | dict[str, Any]) -> None:
        return

    def _on_FraudScreeningCompleted(self, event: StoredEvent | dict[str, Any]) -> None:
        return

    def _on_ComplianceCheckRequested(self, event: StoredEvent | dict[str, Any]) -> None:
        payload = payload_from_stored(event)
        self._transition(ApplicationState.COMPLIANCE_REVIEW, STATE_RULE)
        self.required_compliance_checks = rules_evaluate_ids(payload)

    def _on_ComplianceRulePassed(self, event: StoredEvent | dict[str, Any]) -> None:
        rule_id = payload_from_stored(event).get("rule_id")
        if rule_id:
            self.passed_compliance_checks.add(str(rule_id))

    def _on_DecisionRequested(self, event: StoredEvent | dict[str, Any]) -> None:
        self._transition(ApplicationState.PENDING_DECISION, STATE_RULE)

    def _on_DecisionGenerated(self, event: StoredEvent | dict[str, Any]) -> None:
        payload = payload_from_stored(event)
        confidence, recommendation = _decision_confidence_and_recommendation(payload)
        self.assert_decision_recommendation_valid(recommendation, confidence)
        # DecisionRequested may be missing on the loan stream; APPROVE/DECLINE cannot jump
        # COMPLIANCE_REVIEW → APPROVED_PENDING_HUMAN (only PENDING_DECISION is allowed from there).
        if self.state == ApplicationState.COMPLIANCE_REVIEW:
            self._transition(ApplicationState.PENDING_DECISION, STATE_RULE)
        if recommendation == "APPROVE":
            if self.state != ApplicationState.APPROVED_PENDING_HUMAN:
                self._transition(ApplicationState.APPROVED_PENDING_HUMAN, STATE_RULE)
        elif recommendation == "DECLINE":
            if self.state != ApplicationState.DECLINED_PENDING_HUMAN:
                self._transition(ApplicationState.DECLINED_PENDING_HUMAN, STATE_RULE)
        else:
            if (
                self.state != ApplicationState.PENDING_DECISION
                and ApplicationState.PENDING_DECISION in VALID_TRANSITIONS.get(self.state, set())
            ):
                self._transition(ApplicationState.PENDING_DECISION, STATE_RULE)
        self.last_decision_recommendation = recommendation

    def _on_HumanReviewOverride(self, event: StoredEvent | dict[str, Any]) -> None:
        self.credit_analysis_overridden = True

    def _on_HumanReviewRequested(self, event: StoredEvent | dict[str, Any]) -> None:
        return

    def _on_HumanReviewCompleted(self, event: StoredEvent | dict[str, Any]) -> None:
        payload = payload_from_stored(event)
        if bool(payload.get("override")):
            self.credit_analysis_overridden = True
        final_decision = str(payload.get("final_decision", "")).upper()
        if final_decision == "APPROVE":
            self._transition(ApplicationState.FINAL_APPROVED, STATE_RULE)
        elif final_decision == "DECLINE":
            self._transition(ApplicationState.FINAL_DECLINED, STATE_RULE)

    def _on_ApplicationApproved(self, event: StoredEvent | dict[str, Any]) -> None:
        payload = payload_from_stored(event)
        if self.state != ApplicationState.FINAL_APPROVED:
            self._transition(ApplicationState.FINAL_APPROVED, STATE_RULE)
        self.approved_amount_usd = optional_float(payload, "approved_amount_usd")

    def _on_ApplicationDeclined(self, event: StoredEvent | dict[str, Any]) -> None:
        if self.state != ApplicationState.FINAL_DECLINED:
            self._transition(ApplicationState.FINAL_DECLINED, STATE_RULE)

    def _on_ComplianceCheckCompleted(self, event: StoredEvent | dict[str, Any]) -> None:
        return

    def state_display(self) -> str:
        return self.state.value if self.state else "NONE"

    def assert_awaiting_credit_analysis(self) -> None:
        if self.state == ApplicationState.AWAITING_ANALYSIS:
            return
        if self.state == ApplicationState.ANALYSIS_COMPLETE and self.credit_analysis_overridden:
            return
        if self.state == ApplicationState.ANALYSIS_COMPLETE and not self.credit_analysis_overridden:
            self._raise_domain_error("credit_analysis_locked", MODEL_VERSION_RULE)
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            self._raise_domain_error("application_not_awaiting_analysis", STATE_RULE)

    def assert_decision_recommendation_valid(self, recommendation: str, confidence_score: float) -> None:
        if confidence_score < 0.6 and recommendation != "REFER":
            self._raise_domain_error("confidence_floor_violation", CONFIDENCE_RULE)

    def assert_valid_orchestrator_decision(
        self, raw_recommendation: str, confidence_score: float
    ) -> str:
        recommendation = raw_recommendation.upper()
        if confidence_score < 0.6:
            recommendation = "REFER"
        self.assert_decision_recommendation_valid(recommendation, confidence_score)
        return recommendation

    def assert_new_application_stream(self) -> None:
        if self.version != -1:
            self._raise_domain_error(
                "application_already_exists",
                APPLICATION_UNIQUENESS_RULE,
                application_id=self.application_id,
            )

    def effective_decision_recommendation(self, raw_recommendation: str, confidence_score: float) -> str:
        return self.assert_valid_orchestrator_decision(raw_recommendation, confidence_score)

    def assert_may_append_application_approved(
        self,
        compliance: ComplianceRecordAggregate,
        required_checks_override: list[str],
    ) -> None:
        required = (
            set(required_checks_override)
            or self.required_compliance_checks
            or compliance.required_checks
        )
        self.required_compliance_checks = required
        self.assert_compliance_dependency(compliance.passed_checks)

    def assert_compliance_dependency(self, passed_checks: set[str]) -> None:
        if not self.required_compliance_checks.issubset(passed_checks):
            missing = sorted(self.required_compliance_checks - passed_checks)
            self._raise_domain_error(
                "compliance_dependency_failed",
                COMPLIANCE_RULE,
                missing_checks=missing,
            )
