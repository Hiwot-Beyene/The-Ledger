from __future__ import annotations

from datetime import UTC, datetime

import pytest

from event_store import InMemoryEventStore
from mcp.server import CallerContext, LedgerMCPService


class StubProjectionQueries:
    async def get_application_summary(self, application_id: str):
        return {"application_id": application_id, "state": "PENDING_DECISION"}

    async def get_compliance_current(self, application_id: str):
        return {"application_id": application_id, "verdict": "CLEAR", "rules_evaluated": 1}

    async def get_compliance_as_of(self, application_id: str, as_of: datetime):
        return {"application_id": application_id, "as_of": as_of.isoformat(), "verdict": "CLEAR", "rules_evaluated": 1}

    async def get_agent_performance(self, agent_id: str):
        return [{"agent_id": agent_id, "model_version": "v1", "decisions_generated": 3}]

    async def get_projection_lags(self):
        return {"application_summary": 0, "compliance_audit_view": 0}

    async def get_application_audit_read_model(self, application_id: str):
        return {
            "application_id": application_id,
            "application_summary": await self.get_application_summary(application_id),
            "indexed_agent_sessions": [],
            "decision_attribution": None,
            "source": "stub",
        }

    async def get_agent_session_read_model(self, agent_id: str, session_id: str):
        return {
            "session_id": session_id,
            "agent_id": agent_id,
            "application_id": "stub-app",
            "model_version": "v1",
        }


@pytest.mark.asyncio
async def test_submit_application_calls_handler_path_and_returns_stream_version():
    store = InMemoryEventStore()
    svc = LedgerMCPService(store, projections=StubProjectionQueries())

    out = await svc.submit_application(
        {
            "application_id": "mcp-app-1",
            "applicant_id": "cust-1",
            "requested_amount_usd": "25000",
        },
        CallerContext(role="agent", correlation_id="corr-1"),
    )

    assert out["ok"] is True
    assert out["stream_id"] == "loan-mcp-app-1"
    assert out["initial_version"] == 2


@pytest.mark.asyncio
async def test_record_credit_analysis_requires_active_agent_session():
    store = InMemoryEventStore()
    svc = LedgerMCPService(store, projections=StubProjectionQueries())

    await svc.submit_application(
        {"application_id": "mcp-app-2", "applicant_id": "cust-2", "requested_amount_usd": "1000"},
        CallerContext(),
    )
    rq = await svc.request_credit_analysis({"application_id": "mcp-app-2"}, CallerContext())
    assert rq["ok"] is True

    out = await svc.record_credit_analysis(
        {
            "application_id": "mcp-app-2",
            "agent_id": "credit",
            "session_id": "s-2",
            "model_version": "v1",
            "confidence_score": 0.9,
            "risk_tier": "LOW",
            "recommended_limit_usd": "10000",
            "duration_ms": 10,
            "input_data": {"k": "v"},
        },
        CallerContext(role="agent"),
    )
    assert out["ok"] is False
    assert out["error_type"] == "PreconditionFailed"
    assert out["suggested_action"] == "call_start_agent_session_first"


@pytest.mark.asyncio
async def test_record_human_review_requires_reviewer_role_and_decision_reason():
    store = InMemoryEventStore()
    svc = LedgerMCPService(store, projections=StubProjectionQueries())

    bad_role = await svc.record_human_review(
        {
            "application_id": "mcp-app-3",
            "reviewer_id": "u-1",
            "override": False,
            "original_recommendation": "REFER",
            "final_decision": "APPROVE",
        },
        CallerContext(role="agent"),
    )
    assert bad_role["ok"] is False
    assert bad_role["error_type"] == "AuthorizationError"

    missing_reason = await svc.record_human_review(
        {
            "application_id": "mcp-app-3",
            "reviewer_id": "u-1",
            "override": False,
            "original_recommendation": "REFER",
            "final_decision": "APPROVE",
        },
        CallerContext(role="human_reviewer"),
    )
    assert missing_reason["ok"] is False
    assert missing_reason["error_type"] == "PreconditionFailed"
    assert missing_reason["suggested_action"] == "provide_decision_reason"


@pytest.mark.asyncio
async def test_resources_are_projection_backed():
    store = InMemoryEventStore()
    svc = LedgerMCPService(store, projections=StubProjectionQueries())

    app = await svc.resource_application("app-r-1")
    compliance = await svc.resource_compliance("app-r-1", as_of=datetime.now(UTC))
    perf = await svc.resource_agent_performance("credit")
    health = await svc.resource_health()
    audit = await svc.resource_audit_trail("app-r-1")
    sess = await svc.resource_agent_session("credit", "s-x")

    assert app["ok"] is True
    assert app["data"]["application_id"] == "app-r-1"
    assert compliance["ok"] is True
    assert perf["ok"] is True
    assert health["ok"] is True
    assert "projection_lag" in health
    assert health.get("slo_projection_catchup_ok") is True
    assert audit["ok"] is True
    assert sess["ok"] is True


@pytest.mark.asyncio
async def test_integrity_check_enforces_role_and_rate_limit():
    store = InMemoryEventStore()
    svc = LedgerMCPService(store, projections=StubProjectionQueries())
    await store.append(
        "loan-app-int-1",
        [{"event_type": "ApplicationSubmitted", "event_version": 1, "payload": {"application_id": "app-int-1"}}],
        expected_version=-1,
    )

    denied = await svc.run_integrity_check("loan", "app-int-1", CallerContext(role="agent"))
    assert denied["ok"] is False
    assert denied["error_type"] == "AuthorizationError"

    allowed = await svc.run_integrity_check("loan", "app-int-1", CallerContext(role="compliance"))
    assert allowed["ok"] is True
    assert isinstance(allowed["chain_valid"], bool)

    limited = await svc.run_integrity_check("loan", "app-int-1", CallerContext(role="compliance"))
    assert limited["ok"] is False
    assert limited["error_type"] == "RateLimited"
