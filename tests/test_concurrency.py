import asyncio

import pytest

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError


def _event(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


@pytest.mark.asyncio
async def test_double_decision_concurrency_collision():
    store = InMemoryEventStore()
    stream_id = "loan-APEX-1001"

    await store.append(stream_id, [_event("ApplicationSubmitted")], expected_version=-1)
    await store.append(stream_id, [_event("CreditAnalysisRequested")], expected_version=1)
    await store.append(stream_id, [_event("FraudScreeningRequested")], expected_version=2)

    async def append_decision(agent: str):
        return await store.append(
            stream_id=stream_id,
            events=[_event("CreditAnalysisCompleted", agent=agent)],
            expected_version=3,
            correlation_id=f"corr-{agent}",
        )

    results = await asyncio.gather(
        append_decision("agent-a"),
        append_decision("agent-b"),
        return_exceptions=True,
    )

    events = await store.load_stream(stream_id)
    winners = [e for e in events if e.event_type == "CreditAnalysisCompleted"]
    failures = [r for r in results if isinstance(r, OptimisticConcurrencyError)]

    assert len(events) == 4
    assert len(winners) == 1 and winners[0].stream_position == 4
    assert len(failures) == 1
    assert failures[0].expected_version == 3
    assert failures[0].actual_version == 4
