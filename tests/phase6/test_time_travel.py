from __future__ import annotations

from datetime import UTC, datetime

import pytest

from event_store import InMemoryEventStore
from integrity import run_integrity_check
from regulatory.package import generate_regulatory_package, verify_regulatory_package
from what_if.projector import run_what_if


@pytest.mark.asyncio
async def test_run_what_if_changes_risk_tier_in_projection() -> None:
    store = InMemoryEventStore()
    app_id = "tt-whatif-1"
    stream_id = f"loan-{app_id}"
    now = datetime.now(UTC).isoformat()
    await store.append(
        stream_id,
        [
            {
                "event_type": "ApplicationSubmitted",
                "event_version": 1,
                "payload": {"application_id": app_id, "submitted_at": now},
            },
            {
                "event_type": "CreditAnalysisCompleted",
                "event_version": 2,
                "payload": {
                    "application_id": app_id,
                    "completed_at": now,
                    "risk_tier": "MEDIUM",
                    "recommended_limit_usd": "10000",
                },
            },
        ],
        expected_version=-1,
    )
    result = await run_what_if(
        store,
        app_id,
        "CreditAnalysisCompleted",
        [
            {
                "event_type": "CreditAnalysisCompleted",
                "event_version": 2,
                "payload": {
                    "application_id": app_id,
                    "completed_at": now,
                    "risk_tier": "HIGH",
                    "recommended_limit_usd": "5000",
                },
            },
        ],
    )
    assert result.real_outcome["application_summary"]["risk_tier"] == "MEDIUM"
    assert result.counterfactual_outcome["application_summary"]["risk_tier"] == "HIGH"
    assert any(d.reason == "outcome_diverged" for d in result.divergence_events)


@pytest.mark.asyncio
async def test_generate_regulatory_package_flags_tamper_when_audit_chain_breaks() -> None:
    store = InMemoryEventStore()
    app_id = "tt-reg-1"
    stream_id = f"loan-{app_id}"
    now = datetime.now(UTC).isoformat()
    await store.append(
        stream_id,
        [
            {
                "event_type": "ApplicationSubmitted",
                "event_version": 1,
                "payload": {"application_id": app_id, "submitted_at": now},
            },
        ],
        expected_version=-1,
    )
    await run_integrity_check(store, "loan", app_id)
    store._streams[stream_id][0].payload["application_id"] = "tampered"  # type: ignore[attr-defined]

    pkg = await generate_regulatory_package(store, app_id, datetime.now(UTC))
    assert pkg["audit_integrity"]["tamper_detected"] is True
    assert pkg["audit_integrity"]["chain_valid"] is False


@pytest.mark.asyncio
async def test_verify_regulatory_package_fails_when_body_tampered() -> None:
    store = InMemoryEventStore()
    app_id = "tt-verify-1"
    stream_id = f"loan-{app_id}"
    now = datetime.now(UTC).isoformat()
    await store.append(
        stream_id,
        [
            {
                "event_type": "ApplicationSubmitted",
                "event_version": 1,
                "payload": {"application_id": app_id, "submitted_at": now},
            },
        ],
        expected_version=-1,
    )
    pkg = await generate_regulatory_package(store, app_id, datetime.now(UTC))
    ok, msg = verify_regulatory_package(pkg)
    assert ok is True
    assert msg == ""

    forged = dict(pkg)
    forged["narrative"] = ["forged line"]
    ok2, msg2 = verify_regulatory_package(forged)
    assert ok2 is False
    assert "package_sha256" in msg2
