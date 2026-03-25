import asyncio

import pytest

from event_store import InMemoryEventStore, OptimisticConcurrencyError


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
    successful_appends = [r for r in results if isinstance(r, int)]

    print(
        "\n--- OCC evidence: test_double_decision_concurrency_collision ---\n"
        f"  stream_id: {stream_id}\n"
        f"  total_events_in_stream_after_race: {len(events)}  (expected 4, not 5)\n"
        f"  concurrent_tasks: 2\n"
        f"  append_calls_returned_int_success: {len(successful_appends)}  (expected exactly 1)\n"
        f"  append_calls_raised_OptimisticConcurrencyError: {len(failures)}  (expected exactly 1)\n"
        f"  CreditAnalysisCompleted_events_in_stream: {len(winners)}  (expected 1 at stream_position 4)\n"
    )
    if successful_appends:
        print(f"  winning_append_new_stream_version: {successful_appends[0]}\n")
    if failures:
        e = failures[0]
        print(
            f"  loser_OptimisticConcurrencyError.stream_id: {e.stream_id!r}\n"
            f"  loser_OptimisticConcurrencyError.expected_version: {e.expected_version}  (expected 3)\n"
            f"  loser_OptimisticConcurrencyError.actual_version: {e.actual_version}  (expected 4)\n"
        )
    if winners:
        print(f"  winner_event.stream_position: {winners[0].stream_position}  (expected 4)\n")

    assert len(events) == 4
    assert len(winners) == 1 and winners[0].stream_position == 4
    assert len(failures) == 1
    assert failures[0].expected_version == 3
    assert failures[0].actual_version == 4
