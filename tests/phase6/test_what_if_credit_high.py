from __future__ import annotations

from datetime import UTC, datetime

import pytest

from event_store import InMemoryEventStore
from what_if.projector import TimeTravelError
from what_if_credit_high import (
    _tier_from_credit_payload,
    run_credit_medium_to_high_what_if,
)


@pytest.mark.asyncio
async def test_medium_to_high_changes_summary_when_causal_chain_present() -> None:
    store = InMemoryEventStore()
    app_id = "wi-credit-bonus-1"
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
                    "decision": {
                        "risk_tier": "MEDIUM",
                        "recommended_limit_usd": "100000.0",
                    },
                },
            },
        ],
        expected_version=-1,
    )
    credit = (await store.load_stream(stream_id))[1]
    await store.append(
        stream_id,
        [
            {
                "event_type": "DecisionGenerated",
                "event_version": 2,
                "payload": {"application_id": app_id, "recommendation": "APPROVE", "generated_at": now},
            }
        ],
        expected_version=2,
        causation_id=str(credit.event_id),
    )
    decision_evt = (await store.load_stream(stream_id))[2]
    await store.append(
        stream_id,
        [
            {
                "event_type": "ApplicationApproved",
                "event_version": 1,
                "payload": {"application_id": app_id, "approved_amount_usd": "100000", "approved_at": now},
            }
        ],
        expected_version=3,
        causation_id=str(decision_evt.event_id),
    )

    out = await run_credit_medium_to_high_what_if(store, app_id)
    assert out["real_application_summary"]["decision"] == "APPROVE"
    assert out["counterfactual_application_summary"]["risk_tier"] == "HIGH"
    assert out["counterfactual_application_summary"]["decision"] is None
    assert out["causal_policy"]["decision_events_excluded"] is True
    assert out["causal_policy"].get("application_approved_excluded") is True
    assert out["application_summary_field_diff"]


def test_tier_from_nested_decision() -> None:
    assert _tier_from_credit_payload({"decision": {"risk_tier": "MEDIUM"}}) == "MEDIUM"


@pytest.mark.asyncio
async def test_rejects_already_high() -> None:
    store = InMemoryEventStore()
    app_id = "wi-high-only"
    now = datetime.now(UTC).isoformat()
    await store.append(
        f"loan-{app_id}",
        [
            {
                "event_type": "CreditAnalysisCompleted",
                "event_version": 2,
                "payload": {"application_id": app_id, "decision": {"risk_tier": "HIGH"}, "completed_at": now},
            },
        ],
        expected_version=-1,
    )
    with pytest.raises(TimeTravelError, match="already HIGH"):
        await run_credit_medium_to_high_what_if(store, app_id)
