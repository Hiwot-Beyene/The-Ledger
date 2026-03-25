"""Counterfactual commands and concurrent handler races on InMemoryEventStore."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from aggregates.loan_application import LoanApplicationAggregate
from commands.handlers import (
    CreditAnalysisCompletedCommand,
    DecisionGeneratedCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
    handle_credit_analysis_completed,
    handle_decision_generated,
    handle_start_agent_session,
    handle_submit_application,
)
from event_store import InMemoryEventStore
from models.events import (
    CreditAnalysisRequested,
    ComplianceCheckRequested,
    DecisionRequested,
    DomainError,
    OptimisticConcurrencyError,
)

pytestmark = [pytest.mark.command_handler]


class BarrierInMemoryEventStore(InMemoryEventStore):
    """Call ``arm_barrier`` only for the race window so seed appends do not block."""

    def __init__(self) -> None:
        super().__init__()
        self._before_append = None

    def arm_barrier(self, parties: int = 2) -> None:
        b = asyncio.Barrier(parties)
        self._before_append = b.wait

    async def append(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        if self._before_append is not None:
            await self._before_append()
        return await super().append(*args, **kwargs)


async def _seed_ready_for_credit(store: InMemoryEventStore, app_id: str) -> None:
    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="c1",
            requested_amount_usd=1000,
        ),
        store,
    )
    v = await store.stream_version(f"loan-{app_id}")
    await store.append(
        f"loan-{app_id}",
        [
            CreditAnalysisRequested(
                application_id=app_id,
                requested_at=datetime.now(UTC),
                requested_by="test",
            ).to_store_dict()
        ],
        expected_version=v,
    )
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id="sess-conc",
            agent_id="credit",
            agent_type="credit_analysis",
            application_id=app_id,
            model_version="v1",
        ),
        store,
    )


@pytest.mark.asyncio
async def test_counterfactual_submit_duplicate_application_rejected():
    store = InMemoryEventStore()
    cmd = SubmitApplicationCommand(application_id="CF-1", applicant_id="a", requested_amount_usd=1)
    await handle_submit_application(cmd, store)
    with pytest.raises(DomainError) as exc:
        await handle_submit_application(cmd, store)
    assert exc.value.code == "application_already_exists"


@pytest.mark.asyncio
async def test_counterfactual_credit_wrong_phase_rejected():
    store = InMemoryEventStore()
    app_id = "CF-2"
    await handle_submit_application(
        SubmitApplicationCommand(application_id=app_id, applicant_id="a", requested_amount_usd=1),
        store,
    )
    await handle_start_agent_session(
        StartAgentSessionCommand(
            session_id="s",
            agent_id="credit",
            agent_type="credit_analysis",
            application_id=app_id,
            model_version="v1",
        ),
        store,
    )
    cmd = CreditAnalysisCompletedCommand(
        application_id=app_id,
        agent_id="credit",
        session_id="s",
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


@pytest.mark.asyncio
async def test_concurrent_credit_completion_exactly_one_event():
    store = BarrierInMemoryEventStore()
    app_id = "CF-CONC-CREDIT"
    await _seed_ready_for_credit(store, app_id)
    store.arm_barrier(2)
    cmd = CreditAnalysisCompletedCommand(
        application_id=app_id,
        agent_id="credit",
        session_id="sess-conc",
        model_version="v1",
        confidence_score=0.91,
        risk_tier="LOW",
        recommended_limit_usd=5000,
        duration_ms=12,
        input_data={"k": 1},
    )
    results = await asyncio.gather(
        handle_credit_analysis_completed(cmd, store),
        handle_credit_analysis_completed(cmd, store),
        return_exceptions=True,
    )
    assert sum(1 for r in results if r is None) == 1
    assert sum(1 for r in results if isinstance(r, OptimisticConcurrencyError)) == 1
    loan = await store.load_stream(f"loan-{app_id}")
    assert sum(1 for e in loan if e.event_type == "CreditAnalysisCompleted") == 1
    app = await LoanApplicationAggregate.load(store, app_id)
    assert app.credit_analysis_completed is True


@pytest.mark.asyncio
async def test_concurrent_decision_generated_exactly_one_decision_event():
    store = BarrierInMemoryEventStore()
    app_id = "CF-CONC-DEC"
    agent_stream = "agent-credit-sess-conc"
    await _seed_ready_for_credit(store, app_id)
    await handle_credit_analysis_completed(
        CreditAnalysisCompletedCommand(
            application_id=app_id,
            agent_id="credit",
            session_id="sess-conc",
            model_version="v1",
            confidence_score=0.92,
            risk_tier="LOW",
            recommended_limit_usd=4000,
            duration_ms=10,
            input_data={},
        ),
        store,
    )
    av = await store.stream_version(agent_stream)
    await store.append(
        agent_stream,
        [
            {
                "event_type": "CreditAnalysisCompleted",
                "event_version": 1,
                "payload": {"application_id": app_id},
            }
        ],
        expected_version=av,
    )
    v = await store.stream_version(f"loan-{app_id}")
    trig = "trig-1"
    now = datetime.now(UTC)
    await store.append(
        f"loan-{app_id}",
        [
            ComplianceCheckRequested(
                application_id=app_id,
                requested_at=now,
                triggered_by_event_id=trig,
                regulation_set_version="1.0",
                rules_to_evaluate=[],
            ).to_store_dict(),
            DecisionRequested(
                application_id=app_id,
                requested_at=now,
                all_analyses_complete=True,
                triggered_by_event_id=trig,
            ).to_store_dict(),
        ],
        expected_version=v,
    )
    cmd = DecisionGeneratedCommand(
        application_id=app_id,
        recommendation="REFER",
        confidence_score=0.85,
        contributing_agent_sessions=[agent_stream],
        summary="concurrent-decision",
    )
    store.arm_barrier(2)
    results = await asyncio.gather(
        handle_decision_generated(cmd, store),
        handle_decision_generated(cmd, store),
        return_exceptions=True,
    )
    assert sum(1 for r in results if r is None) == 1
    assert sum(1 for r in results if isinstance(r, OptimisticConcurrencyError)) == 1
    loan = await store.load_stream(f"loan-{app_id}")
    assert sum(1 for e in loan if e.event_type == "DecisionGenerated") == 1


@pytest.mark.asyncio
async def test_parallel_replay_invariant_single_credit_after_race():
    store = BarrierInMemoryEventStore()
    app_id = "CF-REPLAY"
    await _seed_ready_for_credit(store, app_id)
    store.arm_barrier(2)
    cmd = CreditAnalysisCompletedCommand(
        application_id=app_id,
        agent_id="credit",
        session_id="sess-conc",
        model_version="v1",
        confidence_score=0.88,
        risk_tier="MEDIUM",
        recommended_limit_usd=3000,
        duration_ms=8,
        input_data={},
    )
    await asyncio.gather(
        handle_credit_analysis_completed(cmd, store),
        handle_credit_analysis_completed(cmd, store),
        return_exceptions=True,
    )
    agg = await LoanApplicationAggregate.load(store, app_id)
    assert agg.credit_analysis_completed is True
    assert agg.state is not None
