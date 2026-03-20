"""Application command handlers: replay aggregates → domain guards → validated event dicts → append(OCC, trace).

All public ``handle_*`` entrypoints run inside :func:`run_with_command_observability` (logger ``ledger.commands``).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.domain.commands.observability import run_with_command_observability
from ledger.schema.event_factories import (
    store_dict_application_approved,
    store_dict_application_submitted,
    store_dict_credit_analysis_completed_for_loan_stream,
    store_dict_decision_generated_for_loan_stream,
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
            )
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
        for stream_id in cmd.contributing_agent_sessions:
            agent_id, session_id = AgentSessionAggregate.parse_contributing_stream_id(
                stream_id,
                application_id=cmd.application_id,
                loan_application_state=app.state_display(),
            )
            agent = await AgentSessionAggregate.load(store, agent_id, session_id)
            agent.assert_context_loaded()
            agent.assert_session_processed_application(cmd.application_id)

        recommendation = app.effective_decision_recommendation(
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
