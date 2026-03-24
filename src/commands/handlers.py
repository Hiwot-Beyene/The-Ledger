"""Application command handlers: replay aggregates → domain guards → validated event dicts → append(OCC, trace).

All public ``handle_*`` entrypoints run inside :func:`run_with_command_observability` (logger ``ledger.commands``).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from aggregates.agent_session import (
    MODEL_VERSION_RULE as AGENT_SESSION_MV_RULE,
    AgentSessionAggregate,
)
from aggregates.compliance_record import ComplianceRecordAggregate
from aggregates.loan_application import LoanApplicationAggregate
from commands.observability import run_with_command_observability
from models.event_factories import (
    store_dict_application_approved,
    store_dict_application_submitted,
    store_dict_credit_analysis_completed_for_loan_stream,
    store_dict_decision_generated_for_loan_stream,
    store_dict_document_upload_requested,
)
from models.events import (
    AgentContextLoaded,
    AgentSessionStarted,
    ComplianceCheckCompleted,
    ComplianceVerdict,
    DomainError,
    FraudScreeningCompleted,
    HumanReviewCompleted,
)


class EventStoreLike(Protocol):
    async def append(
        self,
        stream_id: str,
        events: list[dict[str, Any]],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int: ...

    async def load_stream(self, stream_id: str) -> list[Any]: ...


def hash_inputs(payload: Any) -> str:
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class SubmitApplicationCommand:
    application_id: str
    applicant_id: str
    requested_amount_usd: Decimal | float | int
    loan_purpose: str | None = None
    loan_term_months: int | None = None
    submission_channel: str | None = None
    contact_email: str | None = None
    contact_name: str | None = None
    application_reference: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(slots=True)
class CreditAnalysisCompletedCommand:
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float
    risk_tier: str
    recommended_limit_usd: Decimal | float | int
    duration_ms: int
    input_data: Any
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(slots=True)
class DecisionGeneratedCommand:
    application_id: str
    recommendation: str
    confidence_score: float
    contributing_agent_sessions: list[str]
    summary: str
    contributing_session_model_versions: dict[str, str] = field(default_factory=dict)
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(slots=True)
class FraudScreeningCompletedCommand:
    application_id: str
    session_id: str
    fraud_score: float
    risk_level: str
    anomalies_found: int
    recommendation: str
    model_version: str
    input_data: Any
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(slots=True)
class ComplianceCheckCompletedCommand:
    application_id: str
    session_id: str
    rules_evaluated: int
    rules_passed: int
    rules_failed: int
    rules_noted: int
    has_hard_block: bool
    overall_verdict: str
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(slots=True)
class ApplicationApprovedCommand:
    application_id: str
    approved_amount_usd: Decimal | float | int
    approved_by: str
    interest_rate_pct: float
    term_months: int
    required_checks: list[str]
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(slots=True)
class StartAgentSessionCommand:
    session_id: str
    agent_id: str
    agent_type: str
    application_id: str
    model_version: str
    context_source: str = "fresh"
    context_token_count: int = 0
    langgraph_graph_version: str = "1.0.0"
    correlation_id: str | None = None
    causation_id: str | None = None


@dataclass(slots=True)
class HumanReviewCompletedCommand:
    application_id: str
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    override_reason: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None


async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store: EventStoreLike,
) -> None:
    async def _body() -> None:
        app = await LoanApplicationAggregate.load(store, cmd.application_id)
        app.assert_new_application_stream()
        new_events = [
            store_dict_application_submitted(
                cmd.application_id,
                cmd.applicant_id,
                cmd.requested_amount_usd,
                submitted_at=datetime.now(UTC),
                loan_purpose=cmd.loan_purpose,
                loan_term_months=cmd.loan_term_months,
                submission_channel=cmd.submission_channel,
                contact_email=cmd.contact_email,
                contact_name=cmd.contact_name,
                application_reference=cmd.application_reference,
            ),
            store_dict_document_upload_requested(cmd.application_id),
        ]
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=new_events,
            expected_version=app.version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    await run_with_command_observability("handle_submit_application", cmd, _body)


async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store: EventStoreLike,
) -> None:
    async def _body() -> None:
        app = await LoanApplicationAggregate.load(store, cmd.application_id)
        agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)
        app.assert_awaiting_credit_analysis()
        agent.assert_context_loaded()
        agent.assert_model_version_current(cmd.model_version)
        new_events = [
            store_dict_credit_analysis_completed_for_loan_stream(
                application_id=cmd.application_id,
                agent_id=cmd.agent_id,
                session_id=cmd.session_id,
                model_version=cmd.model_version,
                confidence_score=float(cmd.confidence_score),
                risk_tier=cmd.risk_tier,
                recommended_limit_usd=cmd.recommended_limit_usd,
                analysis_duration_ms=cmd.duration_ms,
                input_data_hash=hash_inputs(cmd.input_data),
                completed_at=datetime.now(UTC),
            )
        ]
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=new_events,
            expected_version=app.version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    await run_with_command_observability("handle_credit_analysis_completed", cmd, _body)


async def handle_decision_generated(
    cmd: DecisionGeneratedCommand,
    store: EventStoreLike,
) -> None:
    async def _body() -> None:
        app = await LoanApplicationAggregate.load(store, cmd.application_id)
        model_vers = dict(cmd.contributing_session_model_versions)
        session_agents: list[tuple[str, AgentSessionAggregate]] = []
        for stream_id in cmd.contributing_agent_sessions:
            agent_id, session_id = AgentSessionAggregate.parse_contributing_stream_id(
                stream_id,
                application_id=cmd.application_id,
                loan_application_state=app.state_display(),
            )
            agent = await AgentSessionAggregate.load(store, agent_id, session_id)
            session_agents.append((stream_id, agent))
            if stream_id not in model_vers and agent.model_version:
                model_vers[stream_id] = agent.model_version
        for stream_id, agent in session_agents:
            agent.assert_context_loaded()
            agent.assert_session_processed_application(cmd.application_id)
            expected = model_vers.get(stream_id)
            if not expected:
                raise DomainError(
                    code="contributing_session_model_versions_required",
                    message=(
                        f"model_version required for contributing session {stream_id!r} "
                        "(declare in command or establish via AgentContextLoaded)"
                    ),
                    aggregate_id=cmd.application_id,
                    current_state=app.state_display(),
                    rule=AGENT_SESSION_MV_RULE,
                )
            agent.assert_model_version_current(expected)

        recommendation = app.assert_valid_orchestrator_decision(
            cmd.recommendation, cmd.confidence_score
        )
        new_events = [
            store_dict_decision_generated_for_loan_stream(
                application_id=cmd.application_id,
                recommendation=recommendation,
                confidence_score=float(cmd.confidence_score),
                contributing_agent_sessions=cmd.contributing_agent_sessions,
                executive_summary=cmd.summary,
                generated_at=datetime.now(UTC),
            )
        ]
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=new_events,
            expected_version=app.version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    await run_with_command_observability("handle_decision_generated", cmd, _body)


async def handle_fraud_screening_completed(
    cmd: FraudScreeningCompletedCommand,
    store: EventStoreLike,
) -> None:
    async def _body() -> None:
        app = await LoanApplicationAggregate.load(store, cmd.application_id)
        new_events = [
            FraudScreeningCompleted(
                application_id=cmd.application_id,
                session_id=cmd.session_id,
                fraud_score=float(cmd.fraud_score),
                risk_level=cmd.risk_level,
                anomalies_found=int(cmd.anomalies_found),
                recommendation=cmd.recommendation,
                screening_model_version=cmd.model_version,
                input_data_hash=hash_inputs(cmd.input_data),
                completed_at=datetime.now(UTC),
            ).to_store_dict()
        ]
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=new_events,
            expected_version=app.version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    await run_with_command_observability("handle_fraud_screening_completed", cmd, _body)


async def handle_compliance_check_completed(
    cmd: ComplianceCheckCompletedCommand,
    store: EventStoreLike,
) -> None:
    async def _body() -> None:
        app = await LoanApplicationAggregate.load(store, cmd.application_id)
        new_events = [
            ComplianceCheckCompleted(
                application_id=cmd.application_id,
                session_id=cmd.session_id,
                rules_evaluated=int(cmd.rules_evaluated),
                rules_passed=int(cmd.rules_passed),
                rules_failed=int(cmd.rules_failed),
                rules_noted=int(cmd.rules_noted),
                has_hard_block=bool(cmd.has_hard_block),
                overall_verdict=ComplianceVerdict(str(cmd.overall_verdict).upper()),
                completed_at=datetime.now(UTC),
            ).to_store_dict()
        ]
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=new_events,
            expected_version=app.version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    await run_with_command_observability("handle_compliance_check_completed", cmd, _body)


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand,
    store: EventStoreLike,
) -> None:
    async def _body() -> None:
        stream_id = f"agent-{cmd.agent_id}-{cmd.session_id}"
        expected = len(await store.load_stream(stream_id)) - 1
        now = datetime.now(UTC)
        new_events = [
            AgentContextLoaded(
                session_id=cmd.session_id,
                agent_id=cmd.agent_id,
                application_id=cmd.application_id,
                context_source=cmd.context_source,
                model_version=cmd.model_version,
                loaded_at=now,
            ).to_store_dict(),
            AgentSessionStarted(
                session_id=cmd.session_id,
                agent_type=cmd.agent_type,
                agent_id=cmd.agent_id,
                application_id=cmd.application_id,
                model_version=cmd.model_version,
                langgraph_graph_version=cmd.langgraph_graph_version,
                context_source=cmd.context_source,
                context_token_count=cmd.context_token_count,
                started_at=now,
            ).to_store_dict(),
        ]
        await store.append(
            stream_id=stream_id,
            events=new_events,
            expected_version=expected,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    await run_with_command_observability("handle_start_agent_session", cmd, _body)


async def handle_human_review_completed(
    cmd: HumanReviewCompletedCommand,
    store: EventStoreLike,
) -> None:
    async def _body() -> None:
        app = await LoanApplicationAggregate.load(store, cmd.application_id)
        new_events = [
            HumanReviewCompleted(
                application_id=cmd.application_id,
                reviewer_id=cmd.reviewer_id,
                override=cmd.override,
                original_recommendation=cmd.original_recommendation,
                final_decision=cmd.final_decision,
                override_reason=cmd.override_reason,
                reviewed_at=datetime.now(UTC),
            ).to_store_dict()
        ]
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=new_events,
            expected_version=app.version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    await run_with_command_observability("handle_human_review_completed", cmd, _body)


async def handle_application_approved(
    cmd: ApplicationApprovedCommand,
    store: EventStoreLike,
) -> None:
    async def _body() -> None:
        app = await LoanApplicationAggregate.load(store, cmd.application_id)
        compliance = await ComplianceRecordAggregate.load(store, cmd.application_id)
        app.assert_may_append_application_approved(compliance, cmd.required_checks)
        new_events = [
            store_dict_application_approved(
                application_id=cmd.application_id,
                approved_amount_usd=cmd.approved_amount_usd,
                approved_by=cmd.approved_by,
                interest_rate_pct=cmd.interest_rate_pct,
                term_months=cmd.term_months,
                approved_at=datetime.now(UTC),
            )
        ]
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=new_events,
            expected_version=app.version,
            correlation_id=cmd.correlation_id,
            causation_id=cmd.causation_id,
        )

    await run_with_command_observability("handle_application_approved", cmd, _body)
