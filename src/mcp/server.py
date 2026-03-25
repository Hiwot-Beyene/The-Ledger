"""MCP ledger service: command tools, projection-backed resources, structured errors.

**Tools (LLM workflow order where applicable)** — each returns ``{ok, ...}`` or ``{ok: false, error_type, message, suggested_action, context}``:

1. ``submit_application`` — Preconditions: new ``application_id``.
2. ``request_credit_analysis`` — After submit; puts loan into awaiting-analysis (required before ``record_credit_analysis``).
3. ``start_agent_session`` — Preconditions: ``session_id``, ``agent_id``, ``application_id``, ``context_source``, ``model_version``.
4. ``record_credit_analysis`` — Preconditions: session started; model matches Gas Town context.
5. ``record_fraud_screening`` — Preconditions: application exists on loan stream.
6. ``initiate_compliance_check`` — After credit (typical); emits ``ComplianceCheckRequested`` (required before ``record_compliance_check`` for state machine).
7. ``record_compliance_check`` — Preconditions: compliance evaluation finished in your stack; loan stream receives completion.
8. ``generate_decision`` — Preconditions: ``CreditAnalysisCompleted`` and ``ComplianceCheckCompleted`` already on loan stream; ``contributing_agent_sessions`` uses ``agent-{agent_id}-{session_id}`` tokens.
9. ``record_human_review`` — Role ``human_reviewer`` or ``admin``; requires ``decision_reason``; binding approve/decline follows prior ``DecisionGenerated``.
10. ``approve_application`` — Role ``compliance``, ``human_reviewer``, or ``admin``; direct ``ApplicationApproved`` when domain allows (compliance deps satisfied).
11. ``run_integrity_check`` — Role ``compliance``/``admin``; rate-limited.
12. ``run_what_if_projection`` — Role ``compliance``/``admin``.
13. ``generate_regulatory_examination_package`` — Role ``compliance``/``admin``.

**Resources** read only via ``ProjectionQueryPort`` (no ``EventStore.load_stream`` / ``load_all`` in ``resource_*`` handlers).
"""
from __future__ import annotations

import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from commands.handlers import (
    ApplicationApprovedCommand,
    ComplianceCheckCompletedCommand,
    CreditAnalysisCompletedCommand,
    DecisionGeneratedCommand,
    FraudScreeningCompletedCommand,
    SubmitApplicationCommand,
    handle_application_approved,
    handle_compliance_check_completed,
    handle_credit_analysis_completed,
    handle_decision_generated,
    handle_fraud_screening_completed,
    handle_submit_application,
)
from event_store import EventStore, InMemoryEventStore
from integrity import run_integrity_check
from models.events import (
    AgentContextLoaded,
    AgentSessionStarted,
    ApplicationApproved,
    ApplicationDeclined,
    DomainError,
    HumanReviewCompleted,
    OptimisticConcurrencyError,
)
from aggregates.compliance_record import ComplianceRecordAggregate
from aggregates.loan_application import LoanApplicationAggregate
from aggregates.replay import payload_from_stored, event_type_from_stored
from models.event_factories import (
    store_dict_compliance_check_requested,
    store_dict_credit_analysis_requested,
)
from regulatory.package import generate_regulatory_package
from what_if.projector import (
    ApplicationSummaryReplayProjection,
    ComplianceAuditReplayProjection,
    TimeTravelError,
    run_what_if,
)

from mcp.resources import (
    InMemoryProjectionQueries,
    ProjectionQueryPort,
    SqlProjectionQueries,
)

StoreT = EventStore | InMemoryEventStore


@dataclass(slots=True)
class CallerContext:
    role: str = "agent"
    correlation_id: str | None = None
    causation_id: str | None = None
    idempotency_key: str | None = None


class SubmitApplicationInput(BaseModel):
    application_id: str
    applicant_id: str
    requested_amount_usd: Decimal = Field(gt=Decimal("0"))
    loan_purpose: str | None = None
    loan_term_months: int | None = None
    submission_channel: str | None = None
    contact_email: str | None = None
    contact_name: str | None = None
    application_reference: str | None = None


class RecordCreditAnalysisInput(BaseModel):
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    risk_tier: str
    recommended_limit_usd: Decimal
    duration_ms: int = Field(ge=0)
    input_data: dict[str, Any] = Field(default_factory=dict)


class GenerateDecisionInput(BaseModel):
    application_id: str
    recommendation: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    contributing_agent_sessions: list[str]
    summary: str
    contributing_session_model_versions: dict[str, str] = Field(default_factory=dict)


class RecordFraudScreeningInput(BaseModel):
    application_id: str
    session_id: str
    fraud_score: float = Field(ge=0.0, le=1.0)
    risk_level: str
    anomalies_found: int = Field(ge=0)
    recommendation: str
    model_version: str
    input_data: dict[str, Any] = Field(default_factory=dict)


class RecordComplianceCheckInput(BaseModel):
    application_id: str
    session_id: str
    rules_evaluated: int = Field(ge=0)
    rules_passed: int = Field(ge=0)
    rules_failed: int = Field(ge=0)
    rules_noted: int = Field(ge=0)
    has_hard_block: bool
    overall_verdict: str


class RecordHumanReviewInput(BaseModel):
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    override_reason: str | None = None
    decision_reason: str | None = None


class WhatIfInput(BaseModel):
    application_id: str
    branch_at_event_type: str
    counterfactual_events: list[dict[str, Any]]


class RegulatoryPackageInput(BaseModel):
    application_id: str
    examination_date: datetime


class RequestCreditAnalysisInput(BaseModel):
    application_id: str
    requested_by: str = "mcp_agent"
    priority: str = "NORMAL"


class InitiateComplianceCheckInput(BaseModel):
    application_id: str
    regulation_set_version: str = "regs-v1"
    rules_to_evaluate: list[str] = Field(default_factory=list)
    triggered_by_event_id: str | None = None


class ApproveApplicationInput(BaseModel):
    application_id: str
    approved_amount_usd: Decimal = Field(gt=Decimal("0"))
    approved_by: str
    interest_rate_pct: float = Field(ge=0.0, le=100.0)
    term_months: int = Field(ge=1, le=600)
    required_checks: list[str] = Field(default_factory=list)


def _structured_error(
    error_type: str,
    message: str,
    *,
    suggested_action: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "ok": False,
        "error_type": error_type,
        "message": message,
        "context": context or {},
        "suggested_action": suggested_action,
    }


class LedgerMCPService:
    def __init__(
        self,
        store: StoreT,
        projections: ProjectionQueryPort | None = None,
        *,
        max_occ_retries: int = 2,
    ) -> None:
        self._store = store
        self._projections = projections or (
            SqlProjectionQueries(store) if isinstance(store, EventStore) else InMemoryProjectionQueries(store)
        )
        self._max_occ_retries = max(0, int(max_occ_retries))
        self._latency_ms: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=200))
        self._errors: dict[str, int] = defaultdict(int)
        self._integrity_last_called_at: dict[str, float] = {}

    def _record_latency(self, name: str, start: float) -> None:
        self._latency_ms[name].append((time.perf_counter() - start) * 1000.0)

    def _observe_error(self, name: str) -> None:
        self._errors[name] += 1

    async def _run_with_occ_retry(self, tool_name: str, op: Any) -> dict[str, Any]:
        attempt = 0
        while True:
            attempt += 1
            try:
                return await op()
            except OptimisticConcurrencyError as exc:
                if attempt > self._max_occ_retries + 1:
                    self._observe_error(tool_name)
                    return _structured_error(
                        "OptimisticConcurrencyError",
                        str(exc),
                        suggested_action=exc.suggested_action,
                        context={
                            "stream_id": exc.stream_id,
                            "expected_version": exc.expected_version,
                            "actual_version": exc.actual_version,
                            "attempts": attempt,
                        },
                    )

    async def _require_agent_session(self, agent_id: str, session_id: str) -> dict[str, Any] | None:
        stream_id = f"agent-{agent_id}-{session_id}"
        events = await self._store.load_stream(stream_id)
        if not events:
            return _structured_error(
                "PreconditionFailed",
                "No active agent session found.",
                suggested_action="call_start_agent_session_first",
                context={"stream_id": stream_id},
            )
        if events[0].event_type != "AgentContextLoaded":
            return _structured_error(
                "PreconditionFailed",
                "AgentContextLoaded must be first event in the session stream.",
                suggested_action="restart_agent_session_with_context",
                context={"stream_id": stream_id, "first_event_type": events[0].event_type},
            )
        return None

    async def submit_application(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            dto = SubmitApplicationInput.model_validate(payload)
            cmd = SubmitApplicationCommand(
                application_id=dto.application_id,
                applicant_id=dto.applicant_id,
                requested_amount_usd=dto.requested_amount_usd,
                loan_purpose=dto.loan_purpose,
                loan_term_months=dto.loan_term_months,
                submission_channel=dto.submission_channel,
                contact_email=dto.contact_email,
                contact_name=dto.contact_name,
                application_reference=dto.application_reference,
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
            )
            result = await self._run_with_occ_retry(
                "submit_application",
                lambda: handle_submit_application(cmd, self._store),
            )
            if result:
                return result
            stream_id = f"loan-{dto.application_id}"
            version = await self._store.stream_version(stream_id)
            return {"ok": True, "stream_id": stream_id, "initial_version": version}
        except ValidationError as exc:
            self._observe_error("submit_application")
            return _structured_error(
                "ValidationError",
                "Input validation failed.",
                suggested_action="fix_input_and_retry",
                context={"details": exc.errors()},
            )
        except DomainError as exc:
            self._observe_error("submit_application")
            return _structured_error(
                "DomainError",
                exc.message,
                suggested_action="inspect_domain_state_before_retry",
                context=exc.to_diagnostic_dict(),
            )
        finally:
            self._record_latency("submit_application", start)

    async def request_credit_analysis(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            dto = RequestCreditAnalysisInput.model_validate(payload)
            stream_id = f"loan-{dto.application_id}"
            expected = await self._store.stream_version(stream_id)
            event_dict = store_dict_credit_analysis_requested(
                dto.application_id,
                requested_by=dto.requested_by,
                priority=dto.priority,
            )
            await self._store.append(
                stream_id,
                [event_dict],
                expected_version=expected,
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
            )
            return {
                "ok": True,
                "stream_id": stream_id,
                "new_stream_version": await self._store.stream_version(stream_id),
            }
        except ValidationError as exc:
            self._observe_error("request_credit_analysis")
            return _structured_error(
                "ValidationError",
                "Input validation failed.",
                suggested_action="fix_input_and_retry",
                context={"details": exc.errors()},
            )
        except Exception as exc:
            self._observe_error("request_credit_analysis")
            return _structured_error(
                type(exc).__name__,
                str(exc),
                suggested_action="verify_application_submitted_then_retry",
            )
        finally:
            self._record_latency("request_credit_analysis", start)

    async def initiate_compliance_check(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            dto = InitiateComplianceCheckInput.model_validate(payload)
            triggered_by = dto.triggered_by_event_id or caller.correlation_id or caller.causation_id or "mcp"
            stream_id = f"loan-{dto.application_id}"
            expected = await self._store.stream_version(stream_id)
            event_dict = store_dict_compliance_check_requested(
                dto.application_id,
                triggered_by_event_id=str(triggered_by),
                regulation_set_version=dto.regulation_set_version,
                rules_to_evaluate=list(dto.rules_to_evaluate),
            )
            await self._store.append(
                stream_id,
                [event_dict],
                expected_version=expected,
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
            )
            return {
                "ok": True,
                "stream_id": stream_id,
                "new_stream_version": await self._store.stream_version(stream_id),
            }
        except ValidationError as exc:
            self._observe_error("initiate_compliance_check")
            return _structured_error(
                "ValidationError",
                "Input validation failed.",
                suggested_action="fix_input_and_retry",
                context={"details": exc.errors()},
            )
        except Exception as exc:
            self._observe_error("initiate_compliance_check")
            return _structured_error(
                type(exc).__name__,
                str(exc),
                suggested_action="complete_credit_analysis_then_retry",
            )
        finally:
            self._record_latency("initiate_compliance_check", start)

    async def start_agent_session(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            required = {"session_id", "agent_id", "application_id", "context_source", "model_version"}
            missing = sorted(required - set(payload.keys()))
            if missing:
                return _structured_error(
                    "ValidationError",
                    "Missing required fields.",
                    suggested_action="provide_all_required_fields",
                    context={"missing_fields": missing},
                )
            stream_id = f"agent-{payload['agent_id']}-{payload['session_id']}"
            expected = await self._store.stream_version(stream_id)
            now = datetime.now(UTC)
            events = [
                AgentContextLoaded(
                    session_id=str(payload["session_id"]),
                    agent_id=str(payload["agent_id"]),
                    application_id=str(payload["application_id"]),
                    context_source=str(payload["context_source"]),
                    model_version=str(payload["model_version"]),
                    loaded_at=now,
                ).to_store_dict(),
                AgentSessionStarted(
                    session_id=str(payload["session_id"]),
                    agent_type=str(payload.get("agent_type", payload["agent_id"])),
                    agent_id=str(payload["agent_id"]),
                    application_id=str(payload["application_id"]),
                    model_version=str(payload["model_version"]),
                    langgraph_graph_version=str(payload.get("langgraph_graph_version", "unknown")),
                    context_source=str(payload["context_source"]),
                    context_token_count=int(payload.get("context_token_count", 0)),
                    started_at=now,
                ).to_store_dict(),
            ]
            new_version = await self._store.append(
                stream_id=stream_id,
                events=events,
                expected_version=expected,
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
                metadata={"idempotency_key": caller.idempotency_key} if caller.idempotency_key else None,
            )
            return {"ok": True, "session_id": payload["session_id"], "context_position": new_version}
        except Exception as exc:
            self._observe_error("start_agent_session")
            return _structured_error(
                type(exc).__name__,
                str(exc),
                suggested_action="inspect_session_payload_and_retry",
            )
        finally:
            self._record_latency("start_agent_session", start)

    async def record_credit_analysis(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            dto = RecordCreditAnalysisInput.model_validate(payload)
            pre = await self._require_agent_session(dto.agent_id, dto.session_id)
            if pre:
                self._observe_error("record_credit_analysis")
                return pre
            cmd = CreditAnalysisCompletedCommand(
                application_id=dto.application_id,
                agent_id=dto.agent_id,
                session_id=dto.session_id,
                model_version=dto.model_version,
                confidence_score=dto.confidence_score,
                risk_tier=dto.risk_tier,
                recommended_limit_usd=dto.recommended_limit_usd,
                duration_ms=dto.duration_ms,
                input_data=dto.input_data,
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
            )
            result = await self._run_with_occ_retry(
                "record_credit_analysis",
                lambda: handle_credit_analysis_completed(cmd, self._store),
            )
            if result:
                return result
            return {"ok": True, "stream_id": f"loan-{dto.application_id}", "new_stream_version": await self._store.stream_version(f"loan-{dto.application_id}")}
        except ValidationError as exc:
            self._observe_error("record_credit_analysis")
            return _structured_error(
                "ValidationError",
                "Input validation failed.",
                suggested_action="fix_input_and_retry",
                context={"details": exc.errors()},
            )
        except DomainError as exc:
            self._observe_error("record_credit_analysis")
            return _structured_error(
                "DomainError",
                exc.message,
                suggested_action="check_preconditions_then_retry",
                context=exc.to_diagnostic_dict(),
            )
        finally:
            self._record_latency("record_credit_analysis", start)

    async def generate_decision(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            dto = GenerateDecisionInput.model_validate(payload)
            loan_events = await self._store.load_stream(f"loan-{dto.application_id}")
            existing = {e.event_type for e in loan_events}
            required = {"CreditAnalysisCompleted", "ComplianceCheckCompleted"}
            missing = sorted(required - existing)
            if missing:
                self._observe_error("generate_decision")
                return _structured_error(
                    "PreconditionFailed",
                    "Required analyses are missing for decision generation.",
                    suggested_action="complete_missing_analyses_first",
                    context={"application_id": dto.application_id, "missing_events": missing},
                )
            cmd = DecisionGeneratedCommand(
                application_id=dto.application_id,
                recommendation=dto.recommendation,
                confidence_score=dto.confidence_score,
                contributing_agent_sessions=dto.contributing_agent_sessions,
                summary=dto.summary,
                contributing_session_model_versions=dict(dto.contributing_session_model_versions),
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
            )
            result = await self._run_with_occ_retry(
                "generate_decision",
                lambda: handle_decision_generated(cmd, self._store),
            )
            if result:
                return result
            return {"ok": True, "decision_id": f"loan-{dto.application_id}", "new_stream_version": await self._store.stream_version(f"loan-{dto.application_id}")}
        except ValidationError as exc:
            self._observe_error("generate_decision")
            return _structured_error(
                "ValidationError",
                "Input validation failed.",
                suggested_action="fix_input_and_retry",
                context={"details": exc.errors()},
            )
        except DomainError as exc:
            self._observe_error("generate_decision")
            return _structured_error(
                "DomainError",
                exc.message,
                suggested_action="reload_context_then_retry",
                context=exc.to_diagnostic_dict(),
            )
        finally:
            self._record_latency("generate_decision", start)

    async def record_fraud_screening(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            dto = RecordFraudScreeningInput.model_validate(payload)
            cmd = FraudScreeningCompletedCommand(
                application_id=dto.application_id,
                session_id=dto.session_id,
                fraud_score=dto.fraud_score,
                risk_level=dto.risk_level,
                anomalies_found=dto.anomalies_found,
                recommendation=dto.recommendation,
                model_version=dto.model_version,
                input_data=dto.input_data,
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
            )
            result = await self._run_with_occ_retry(
                "record_fraud_screening",
                lambda: handle_fraud_screening_completed(cmd, self._store),
            )
            if result:
                return result
            return {
                "ok": True,
                "stream_id": f"loan-{dto.application_id}",
                "new_stream_version": await self._store.stream_version(f"loan-{dto.application_id}"),
            }
        except ValidationError as exc:
            self._observe_error("record_fraud_screening")
            return _structured_error(
                "ValidationError",
                "Input validation failed.",
                suggested_action="fix_input_and_retry",
                context={"details": exc.errors()},
            )
        except DomainError as exc:
            self._observe_error("record_fraud_screening")
            return _structured_error(
                "DomainError",
                exc.message,
                suggested_action="check_preconditions_then_retry",
                context=exc.to_diagnostic_dict(),
            )
        finally:
            self._record_latency("record_fraud_screening", start)

    async def record_compliance_check(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            dto = RecordComplianceCheckInput.model_validate(payload)
            cmd = ComplianceCheckCompletedCommand(
                application_id=dto.application_id,
                session_id=dto.session_id,
                rules_evaluated=dto.rules_evaluated,
                rules_passed=dto.rules_passed,
                rules_failed=dto.rules_failed,
                rules_noted=dto.rules_noted,
                has_hard_block=dto.has_hard_block,
                overall_verdict=dto.overall_verdict,
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
            )
            result = await self._run_with_occ_retry(
                "record_compliance_check",
                lambda: handle_compliance_check_completed(cmd, self._store),
            )
            if result:
                return result
            return {
                "ok": True,
                "stream_id": f"loan-{dto.application_id}",
                "new_stream_version": await self._store.stream_version(f"loan-{dto.application_id}"),
            }
        except ValidationError as exc:
            self._observe_error("record_compliance_check")
            return _structured_error(
                "ValidationError",
                "Input validation failed.",
                suggested_action="fix_input_and_retry",
                context={"details": exc.errors()},
            )
        except DomainError as exc:
            self._observe_error("record_compliance_check")
            return _structured_error(
                "DomainError",
                exc.message,
                suggested_action="check_preconditions_then_retry",
                context=exc.to_diagnostic_dict(),
            )
        finally:
            self._record_latency("record_compliance_check", start)

    async def record_human_review(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            dto = RecordHumanReviewInput.model_validate(payload)
            if caller.role not in {"human_reviewer", "admin"}:
                self._observe_error("record_human_review")
                return _structured_error(
                    "AuthorizationError",
                    "Only human_reviewer or admin can record human review.",
                    suggested_action="use_reviewer_credentials",
                )
            rationale = (dto.decision_reason or dto.override_reason or "").strip()
            if not rationale:
                self._observe_error("record_human_review")
                return _structured_error(
                    "PreconditionFailed",
                    "decision_reason is required (staff rationale for approve or decline).",
                    suggested_action="provide_decision_reason",
                )
            final_decision = str(dto.final_decision).upper()
            if final_decision not in {"APPROVE", "DECLINE"}:
                self._observe_error("record_human_review")
                return _structured_error(
                    "PreconditionFailed",
                    "Human final decision must be APPROVE or DECLINE (binding).",
                    suggested_action="choose a binding final_decision",
                    context={"final_decision": final_decision},
                )

            stream_id = f"loan-{dto.application_id}"
            expected = await self._store.stream_version(stream_id)

            # Binding inputs come from the orchestrator's prior DecisionGenerated event.
            loan_events = await self._store.load_stream(stream_id)
            decision_payload: dict[str, Any] | None = None
            for ev in reversed(loan_events):
                if event_type_from_stored(ev) == "DecisionGenerated":
                    decision_payload = payload_from_stored(ev)
                    break

            reviewed_at = datetime.now(UTC)
            human_event = HumanReviewCompleted(
                application_id=dto.application_id,
                reviewer_id=dto.reviewer_id,
                override=dto.override,
                original_recommendation=dto.original_recommendation,
                final_decision=dto.final_decision,
                override_reason=rationale,
                reviewed_at=reviewed_at,
            ).to_store_dict()

            binding_events: list[dict[str, Any]] = []
            if final_decision == "APPROVE":
                if not decision_payload:
                    self._observe_error("record_human_review")
                    return _structured_error(
                        "PreconditionFailed",
                        "Missing DecisionGenerated; orchestrator recommendation must exist before binding approval.",
                        suggested_action="run workflow until DecisionGenerated exists, then retry",
                    )

                # Validate compliance dependency before emitting binding approval.
                app = await LoanApplicationAggregate.load(self._store, dto.application_id)
                compliance = await ComplianceRecordAggregate.load(self._store, dto.application_id)
                try:
                    app.assert_may_append_application_approved(compliance, required_checks_override=[])
                except DomainError as exc:
                    self._observe_error("record_human_review")
                    return _structured_error(
                        "DomainError",
                        exc.message,
                        suggested_action="fix_preconditions_and_retry",
                        context=exc.to_diagnostic_dict(),
                    )

                approved_amount_raw = decision_payload.get("approved_amount_usd")
                approved_amount_usd = (
                    Decimal(str(approved_amount_raw))
                    if approved_amount_raw is not None
                    else (Decimal(str(app.requested_amount_usd)) if app.requested_amount_usd is not None else None)
                )
                if approved_amount_usd is None:
                    self._observe_error("record_human_review")
                    return _structured_error(
                        "PreconditionFailed",
                        "approved_amount_usd missing; cannot emit ApplicationApproved.",
                        suggested_action="ensure orchestrator includes approved_amount_usd, or provide a scenario that does",
                    )

                conditions = decision_payload.get("conditions") or []
                conditions_list = [str(c) for c in conditions] if isinstance(conditions, list) else []

                binding_events.append(
                    ApplicationApproved(
                        application_id=dto.application_id,
                        approved_amount_usd=approved_amount_usd,
                        interest_rate_pct=9.5,
                        term_months=36,
                        approved_by=dto.reviewer_id,
                        approved_at=reviewed_at,
                        conditions=conditions_list,
                    ).to_store_dict()
                )
            else:
                binding_events.append(
                    ApplicationDeclined(
                        application_id=dto.application_id,
                        decline_reasons=["Risk profile does not meet policy threshold."],
                        declined_by=dto.reviewer_id,
                        adverse_action_notice_required=True,
                        adverse_action_codes=["AUTO_DECLINE"],
                        declined_at=reviewed_at,
                    ).to_store_dict()
                )

            await self._store.append(
                stream_id=stream_id,
                events=[human_event, *binding_events],
                expected_version=expected,
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
                metadata={"idempotency_key": caller.idempotency_key} if caller.idempotency_key else None,
            )

            application_state = "FINAL_APPROVED" if final_decision == "APPROVE" else "FINAL_DECLINED"
            return {
                "ok": True,
                "final_decision": dto.final_decision,
                "application_state": application_state,
            }
        except ValidationError as exc:
            self._observe_error("record_human_review")
            return _structured_error(
                "ValidationError",
                "Input validation failed.",
                suggested_action="fix_input_and_retry",
                context={"details": exc.errors()},
            )
        except OptimisticConcurrencyError as exc:
            self._observe_error("record_human_review")
            return _structured_error(
                "OptimisticConcurrencyError",
                str(exc),
                suggested_action=exc.suggested_action,
                context=exc.to_diagnostic_dict(),
            )
        finally:
            self._record_latency("record_human_review", start)

    async def run_integrity_check(self, entity_type: str, entity_id: str, caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            if caller.role not in {"compliance", "admin"}:
                self._observe_error("run_integrity_check")
                return _structured_error(
                    "AuthorizationError",
                    "Integrity checks are restricted to compliance role.",
                    suggested_action="use_compliance_role_credentials",
                )
            rate_key = f"{entity_type}:{entity_id}"
            now = time.monotonic()
            if now - self._integrity_last_called_at.get(rate_key, 0.0) < 60.0:
                self._observe_error("run_integrity_check")
                return _structured_error(
                    "RateLimited",
                    "Integrity check allowed once per minute per entity.",
                    suggested_action="retry_after_backoff",
                    context={"retry_after_seconds": 60},
                )
            self._integrity_last_called_at[rate_key] = now
            result = await run_integrity_check(self._store, entity_type, entity_id)
            return {
                "ok": True,
                "check_result": {
                    "events_verified": result.events_verified,
                    "integrity_hash": result.integrity_hash,
                    "previous_hash": result.previous_hash,
                    "tamper_detected": result.tamper_detected,
                },
                "chain_valid": result.chain_valid,
            }
        finally:
            self._record_latency("run_integrity_check", start)

    async def resource_application(self, application_id: str) -> dict[str, Any]:
        if self._projections is None:
            return _structured_error(
                "ProjectionUnavailable",
                "Projection query port not configured.",
                suggested_action="wire_projection_queries_or_use_in_memory_store",
            )
        row = await self._projections.get_application_summary(application_id)
        if not row:
            return _structured_error(
                "ReadModelNotReady",
                "No application_summary row / replay result for this id.",
                suggested_action="submit_application_and_progress_workflow_or_retry_later",
                context={"application_id": application_id},
            )
        return {"ok": True, "data": row}

    async def resource_compliance(self, application_id: str, as_of: datetime | None = None) -> dict[str, Any]:
        if self._projections is None:
            return _structured_error(
                "ProjectionUnavailable",
                "Projection query port not configured.",
                suggested_action="wire_projection_queries_or_use_in_memory_store",
            )
        if as_of is not None:
            row = await self._projections.get_compliance_as_of(application_id, as_of)
        else:
            row = await self._projections.get_compliance_current(application_id)
        if not row:
            return _structured_error(
                "ReadModelNotReady",
                "Compliance read model empty for this application (projection not caught up or no compliance completion yet).",
                suggested_action="call_record_compliance_check_then_retry_or_wait_for_projections",
                context={"application_id": application_id, "as_of": as_of.isoformat() if as_of else None},
            )
        return {"ok": True, "source": "projection", "data": row}

    async def resource_agent_performance(self, agent_id: str) -> dict[str, Any]:
        if self._projections is None:
            return _structured_error(
                "ProjectionUnavailable",
                "Projection query port not configured.",
                suggested_action="wire_projection_queries_or_use_in_memory_store",
            )
        return {"ok": True, "data": await self._projections.get_agent_performance(agent_id)}

    async def resource_health(self) -> dict[str, Any]:
        lags = await self._projections.get_projection_lags() if self._projections else {}
        ceiling = int(os.environ.get("LEDGER_PROJECTION_MAX_LAG_EVENTS", "100000"))
        max_lag = max(lags.values(), default=0)
        p95_latency = {
            tool: sorted(samples)[int(max(0, min(len(samples) - 1, (len(samples) * 0.95) - 1)))]
            for tool, samples in self._latency_ms.items()
            if samples
        }
        return {
            "ok": True,
            "projection_lag": lags,
            "max_projection_lag": max_lag,
            "slo_max_lag_events_ceiling": ceiling,
            "slo_projection_catchup_ok": max_lag <= ceiling,
            "tool_latency_ms_p95": p95_latency,
            "tool_error_counts": dict(self._errors),
            "generated_at": datetime.now(UTC).isoformat(),
        }

    async def resource_audit_trail(self, application_id: str) -> dict[str, Any]:
        if self._projections is None:
            return _structured_error(
                "ProjectionUnavailable",
                "Projection query port not configured.",
                suggested_action="wire_projection_queries_or_use_in_memory_store",
            )
        data = await self._projections.get_application_audit_read_model(application_id)
        if not data:
            return _structured_error(
                "ReadModelNotReady",
                "No audit read model for this application.",
                suggested_action="submit_application_first",
                context={"application_id": application_id},
            )
        return {"ok": True, "data": data}

    async def resource_agent_session(self, agent_id: str, session_id: str) -> dict[str, Any]:
        if self._projections is None:
            return _structured_error(
                "ProjectionUnavailable",
                "Projection query port not configured.",
                suggested_action="wire_projection_queries_or_use_in_memory_store",
            )
        data = await self._projections.get_agent_session_read_model(agent_id, session_id)
        if not data:
            return _structured_error(
                "ReadModelNotReady",
                "Agent session not indexed / no AgentSessionStarted observed.",
                suggested_action="call_start_agent_session_then_retry",
                context={"agent_id": agent_id, "session_id": session_id},
            )
        return {"ok": True, "data": data}

    async def approve_application(self, payload: dict[str, Any], caller: CallerContext) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            if caller.role not in {"human_reviewer", "admin", "compliance"}:
                self._observe_error("approve_application")
                return _structured_error(
                    "AuthorizationError",
                    "approve_application requires human_reviewer, admin, or compliance role.",
                    suggested_action="use_authorized_role",
                )
            dto = ApproveApplicationInput.model_validate(payload)
            cmd = ApplicationApprovedCommand(
                application_id=dto.application_id,
                approved_amount_usd=dto.approved_amount_usd,
                approved_by=dto.approved_by,
                interest_rate_pct=float(dto.interest_rate_pct),
                term_months=int(dto.term_months),
                required_checks=list(dto.required_checks),
                correlation_id=caller.correlation_id,
                causation_id=caller.causation_id,
            )
            result = await self._run_with_occ_retry(
                "approve_application",
                lambda: handle_application_approved(cmd, self._store),
            )
            if result:
                return result
            return {"ok": True, "application_state": "FINAL_APPROVED"}
        except ValidationError as exc:
            self._observe_error("approve_application")
            return _structured_error(
                "ValidationError",
                "Input validation failed.",
                suggested_action="fix_input_and_retry",
                context={"details": exc.errors()},
            )
        except DomainError as exc:
            self._observe_error("approve_application")
            return _structured_error(
                "DomainError",
                exc.message,
                suggested_action="inspect_state_and_preconditions_then_retry",
                context=exc.to_diagnostic_dict(),
            )
        except Exception as exc:
            self._observe_error("approve_application")
            return _structured_error(type(exc).__name__, str(exc), suggested_action="inspect_state_and_retry")
        finally:
            self._record_latency("approve_application", start)

    async def run_what_if_projection(
        self, payload: dict[str, Any], caller: CallerContext
    ) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            if caller.role not in {"compliance", "admin"}:
                self._observe_error("run_what_if_projection")
                return _structured_error(
                    "AuthorizationError",
                    "What-if projections are restricted to compliance/admin role.",
                    suggested_action="use_compliance_or_admin_credentials",
                )
            dto = WhatIfInput.model_validate(payload)
            result = await run_what_if(
                self._store,
                dto.application_id,
                dto.branch_at_event_type,
                dto.counterfactual_events,
                projections=[ApplicationSummaryReplayProjection],
            )
            return {
                "ok": True,
                "branch_event_id": result.branch_event_id,
                "real_outcome": result.real_outcome,
                "counterfactual_outcome": result.counterfactual_outcome,
                "divergence_events": [
                    {
                        "reason": d.reason,
                        "event_id": d.event_id,
                        "event_type": d.event_type,
                        "stream_id": d.stream_id,
                    }
                    for d in result.divergence_events
                ],
            }
        except (ValidationError, TimeTravelError, ValueError) as exc:
            self._observe_error("run_what_if_projection")
            return _structured_error(
                type(exc).__name__,
                str(exc),
                suggested_action="validate_branch_and_counterfactual_payload",
            )
        finally:
            self._record_latency("run_what_if_projection", start)

    async def generate_regulatory_examination_package(
        self, payload: dict[str, Any], caller: CallerContext
    ) -> dict[str, Any]:
        start = time.perf_counter()
        try:
            if caller.role not in {"compliance", "admin"}:
                self._observe_error("generate_regulatory_examination_package")
                return _structured_error(
                    "AuthorizationError",
                    "Regulatory package generation is restricted to compliance/admin role.",
                    suggested_action="use_compliance_or_admin_credentials",
                )
            dto = RegulatoryPackageInput.model_validate(payload)
            pkg = await generate_regulatory_package(
                self._store,
                dto.application_id,
                dto.examination_date,
            )
            return {"ok": True, "package": pkg}
        except (ValidationError, TimeTravelError, ValueError) as exc:
            self._observe_error("generate_regulatory_examination_package")
            return _structured_error(
                type(exc).__name__,
                str(exc),
                suggested_action="fix_input_and_retry",
            )
        finally:
            self._record_latency("generate_regulatory_examination_package", start)


def build_fastmcp_server(service: LedgerMCPService) -> Any:
    try:
        from fastmcp import FastMCP  # type: ignore
    except Exception as exc:  # pragma: no cover - dependency/runtime guard
        raise RuntimeError("fastmcp is required to build MCP server runtime.") from exc

    app = FastMCP("the-ledger")

    @app.tool(
        name="submit_application",
        description=(
            "LLM: Create loan stream with ApplicationSubmitted (+ document request). "
            "Preconditions: application_id not used before; requested_amount_usd > 0. "
            "On success ok=true with initial_version (stream position after append). "
            "Next: request_credit_analysis."
        ),
    )
    async def submit_application_tool(payload: dict[str, Any], caller: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.submit_application(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="request_credit_analysis",
        description=(
            "LLM: Emit CreditAnalysisRequested on loan-{application_id}. "
            "Preconditions: submit_application succeeded. "
            "Required before record_credit_analysis (loan must be AwaitingAnalysis). "
            "Errors: DomainError / OptimisticConcurrencyError with suggested_action."
        ),
    )
    async def request_credit_analysis_tool(
        payload: dict[str, Any], caller: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await service.request_credit_analysis(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="start_agent_session",
        description=(
            "LLM: Open agent-{agent_id}-{session_id} with AgentContextLoaded + AgentSessionStarted (Gas Town). "
            "Preconditions: all keys session_id, agent_id, application_id, context_source, model_version; "
            "call before record_credit_analysis for that agent/session. "
            "model_version must match later record_credit_analysis.model_version."
        ),
    )
    async def start_agent_session_tool(payload: dict[str, Any], caller: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.start_agent_session(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="record_credit_analysis",
        description=(
            "LLM: Append CreditAnalysisCompleted. Preconditions: request_credit_analysis done; "
            "start_agent_session for same agent_id/session_id; application awaiting analysis; "
            "confidence 0..1. contributing_agent_sessions for decisions use full stream id "
            "agent-{agent_id}-{session_id}."
        ),
    )
    async def record_credit_analysis_tool(payload: dict[str, Any], caller: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.record_credit_analysis(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="record_fraud_screening",
        description=(
            "LLM: Append FraudScreeningCompleted on loan stream. "
            "Preconditions: application exists; supply session_id, model_version, fraud_score, risk_level, "
            "recommendation, anomalies_found, input_data (can be {}). "
        ),
    )
    async def record_fraud_screening_tool(payload: dict[str, Any], caller: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.record_fraud_screening(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="initiate_compliance_check",
        description=(
            "LLM: Append ComplianceCheckRequested (rules_to_evaluate, regulation_set_version). "
            "Preconditions: typically after credit analysis so state can reach compliance review. "
            "Call before record_compliance_check."
        ),
    )
    async def initiate_compliance_check_tool(
        payload: dict[str, Any], caller: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await service.initiate_compliance_check(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="record_compliance_check",
        description=(
            "LLM: Append ComplianceCheckCompleted. Preconditions: initiate_compliance_check (recommended); "
            "your engine evaluated rules; set overall_verdict (e.g. CLEAR), rule counts, has_hard_block."
        ),
    )
    async def record_compliance_check_tool(payload: dict[str, Any], caller: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.record_compliance_check(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="generate_decision",
        description=(
            "LLM: Append DecisionGenerated. Preconditions: loan stream already has "
            "CreditAnalysisCompleted and ComplianceCheckCompleted; "
            "contributing_agent_sessions list uses tokens agent-{agent_id}-{session_id}; "
            "confidence>=0.6 or recommendation becomes REFER by policy; "
            "optional contributing_session_model_versions map aligns with AgentContextLoaded."
        ),
    )
    async def generate_decision_tool(payload: dict[str, Any], caller: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.generate_decision(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="record_human_review",
        description=(
            "LLM: Binding approve/decline after DecisionGenerated. "
            "Preconditions: role human_reviewer or admin; decision_reason (or override_reason) non-empty; "
            "final_decision APPROVE or DECLINE; if APPROVE, prior DecisionGenerated must exist and "
            "compliance dependency satisfied (or domain error). Orchestrator may omit approved_amount_usd; "
            "ledger may fall back to requested amount."
        ),
    )
    async def record_human_review_tool(payload: dict[str, Any], caller: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.record_human_review(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="approve_application",
        description=(
            "LLM: Append ApplicationApproved via command handler (bypass human wrapper). "
            "Preconditions: role human_reviewer, admin, or compliance; domain allows transition; "
            "compliance required_checks satisfied or pass required_checks override per policy. "
            "Prefer record_human_review when orchestrator emitted DecisionGenerated with human-in-loop."
        ),
    )
    async def approve_application_tool(payload: dict[str, Any], caller: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.approve_application(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="run_integrity_check",
        description=(
            "LLM: Extend audit hash chain for entity. Preconditions: role compliance or admin; "
            "max once/minute per entity (RateLimited else); entity_type e.g. loan, entity_id suffix of stream."
        ),
    )
    async def run_integrity_check_tool(entity_type: str, entity_id: str, caller: dict[str, Any] | None = None) -> dict[str, Any]:
        return await service.run_integrity_check(entity_type, entity_id, CallerContext(**(caller or {})))

    @app.tool(
        name="run_what_if_projection",
        description=(
            "LLM: Counterfactual branch replay. Preconditions: role compliance or admin; "
            "payload application_id, branch_at_event_type, counterfactual_events (list of event dicts); "
            "branch event must exist on application timeline."
        ),
    )
    async def run_what_if_projection_tool(
        payload: dict[str, Any], caller: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await service.run_what_if_projection(payload, CallerContext(**(caller or {})))

    @app.tool(
        name="generate_regulatory_examination_package",
        description=(
            "LLM: Snapshot package for examiners. Preconditions: role compliance or admin; "
            "application_id; examination_date timezone-aware ISO datetime; events must exist for application."
        ),
    )
    async def generate_regulatory_package_tool(
        payload: dict[str, Any], caller: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await service.generate_regulatory_examination_package(payload, CallerContext(**(caller or {})))

    @app.resource("ledger://applications/{application_id}")
    async def application_resource(application_id: str) -> dict[str, Any]:
        return await service.resource_application(application_id)

    @app.resource("ledger://applications/{application_id}/compliance")
    async def compliance_resource(application_id: str, as_of: str | None = None) -> dict[str, Any]:
        dt = datetime.fromisoformat(as_of) if as_of else None
        return await service.resource_compliance(application_id, dt)

    @app.resource("ledger://applications/{application_id}/audit-trail")
    async def audit_trail_resource(application_id: str) -> dict[str, Any]:
        return await service.resource_audit_trail(application_id)

    @app.resource("ledger://agents/{agent_id}/performance")
    async def agent_performance_resource(agent_id: str) -> dict[str, Any]:
        return await service.resource_agent_performance(agent_id)

    @app.resource("ledger://agents/{agent_id}/sessions/{session_id}")
    async def agent_session_resource(agent_id: str, session_id: str) -> dict[str, Any]:
        return await service.resource_agent_session(agent_id, session_id)

    @app.resource("ledger://ledger/health")
    async def health_resource() -> dict[str, Any]:
        return await service.resource_health()

    return app
