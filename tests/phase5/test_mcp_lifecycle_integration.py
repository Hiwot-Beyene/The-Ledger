"""End-to-end ledger flow using only LedgerMCPService (same surface as MCP tools)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from event_store import InMemoryEventStore
from mcp.server import CallerContext, LedgerMCPService


@pytest.mark.asyncio
async def test_full_application_lifecycle_via_mcp_service_only() -> None:
    store = InMemoryEventStore()
    svc = LedgerMCPService(store)
    app_id = "mcp-life-1"
    agent_id, session_id = "credit_analysis", "sess-lc1"

    assert (await svc.submit_application(
        {"application_id": app_id, "applicant_id": "c1", "requested_amount_usd": "8000"},
        CallerContext(role="agent", correlation_id="corr-life"),
    ))["ok"] is True

    assert (await svc.request_credit_analysis({"application_id": app_id}, CallerContext(role="agent")))[
        "ok"
    ] is True

    assert (
        await svc.start_agent_session(
            {
                "session_id": session_id,
                "agent_id": agent_id,
                "application_id": app_id,
                "context_source": "fresh",
                "model_version": "mx-v1",
            },
            CallerContext(role="agent"),
        )
    )["ok"] is True

    assert (
        await svc.record_credit_analysis(
            {
                "application_id": app_id,
                "agent_id": agent_id,
                "session_id": session_id,
                "model_version": "mx-v1",
                "confidence_score": 0.88,
                "risk_tier": "LOW",
                "recommended_limit_usd": "7500",
                "duration_ms": 50,
                "input_data": {},
            },
            CallerContext(role="agent"),
        )
    )["ok"] is True

    assert (
        await svc.record_fraud_screening(
            {
                "application_id": app_id,
                "session_id": "fraud-fs1",
                "fraud_score": 0.12,
                "risk_level": "LOW",
                "anomalies_found": 0,
                "recommendation": "PASS",
                "model_version": "fraud-v2",
                "input_data": {},
            },
            CallerContext(role="agent"),
        )
    )["ok"] is True

    assert (
        await svc.initiate_compliance_check(
            {
                "application_id": app_id,
                "regulation_set_version": "regs-v1",
                "rules_to_evaluate": [],
            },
            CallerContext(role="agent"),
        )
    )["ok"] is True

    assert (
        await svc.record_compliance_check(
            {
                "application_id": app_id,
                "session_id": "comp-s1",
                "rules_evaluated": 1,
                "rules_passed": 1,
                "rules_failed": 0,
                "rules_noted": 0,
                "has_hard_block": False,
                "overall_verdict": "CLEAR",
            },
            CallerContext(role="agent"),
        )
    )["ok"] is True

    assert (
        await svc.generate_decision(
            {
                "application_id": app_id,
                "recommendation": "APPROVE",
                "confidence_score": 0.9,
                "contributing_agent_sessions": [f"agent-{agent_id}-{session_id}"],
                "summary": "Within policy.",
                "contributing_session_model_versions": {},
            },
            CallerContext(role="agent"),
        )
    )["ok"] is True

    hr = await svc.record_human_review(
        {
            "application_id": app_id,
            "reviewer_id": "reviewer-1",
            "override": False,
            "original_recommendation": "APPROVE",
            "final_decision": "APPROVE",
            "decision_reason": "Second line confirms capacity.",
        },
        CallerContext(role="human_reviewer"),
    )
    assert hr["ok"] is True
    assert hr.get("application_state") == "FINAL_APPROVED"

    app_res = await svc.resource_application(app_id)
    assert app_res["ok"] is True
    assert app_res["data"].get("state") in ("FINAL_APPROVED", "APPROVED")

    comp = await svc.resource_compliance(app_id)
    assert comp["ok"] is True

    audit = await svc.resource_audit_trail(app_id)
    assert audit["ok"] is True
    assert len(audit["data"].get("milestones", [])) >= 5

    sess_res = await svc.resource_agent_session(agent_id, session_id)
    assert sess_res["ok"] is True
    assert sess_res["data"]["model_version"] == "mx-v1"

    health = await svc.resource_health()
    assert health["ok"] is True
    assert "slo_projection_catchup_ok" in health
