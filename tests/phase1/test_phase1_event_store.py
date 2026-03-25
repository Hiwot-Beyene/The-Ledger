import asyncio

import pytest

from event_store import EventStore, EventStoreLike, InMemoryEventStore, OptimisticConcurrencyError


def _event(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


def test_event_store_like_runtime_checkable():
    assert isinstance(InMemoryEventStore(), EventStoreLike)
    assert isinstance(EventStore("postgresql://unused"), EventStoreLike)


@pytest.mark.asyncio
async def test_checkpoint_save_load_roundtrip():
    store = InMemoryEventStore()
    assert await store.load_checkpoint("proj-a") == 0
    await store.save_checkpoint("proj-a", 42)
    assert await store.load_checkpoint("proj-a") == 42
    await store.save_checkpoint("proj-a", 99)
    assert await store.load_checkpoint("proj-a") == 99


@pytest.mark.asyncio
async def test_interface_methods_exist_and_basic_flow_works():
    store = InMemoryEventStore()
    assert await store.stream_version("loan-APEX-0001") == -1

    v1 = await store.append(
        "loan-APEX-0001",
        [_event("ApplicationSubmitted", application_id="APEX-0001")],
        expected_version=-1,
        correlation_id="corr-1",
    )
    assert v1 == 1

    v2 = await store.append(
        "loan-APEX-0001",
        [_event("CreditAnalysisRequested", application_id="APEX-0001")],
        expected_version=1,
        causation_id="cause-1",
    )
    assert v2 == 2

    events = await store.load_stream("loan-APEX-0001")
    assert len(events) == 2
    assert events[0].stream_position == 1
    assert events[1].stream_position == 2

    replay = [e async for e in store.load_all(from_global_position=0)]
    assert len(replay) == 2

    metadata = await store.get_stream_metadata("loan-APEX-0001")
    assert metadata.stream_id == "loan-APEX-0001"
    assert metadata.current_version == 2

    await store.archive_stream("loan-APEX-0001")
    archived = await store.get_stream_metadata("loan-APEX-0001")
    assert archived.archived_at is not None


@pytest.mark.asyncio
async def test_append_raises_occ_on_wrong_expected_version():
    store = InMemoryEventStore()
    await store.append("loan-APEX-0002", [_event("ApplicationSubmitted")], expected_version=-1)

    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append("loan-APEX-0002", [_event("CreditAnalysisCompleted")], expected_version=-1)

    assert exc.value.stream_id == "loan-APEX-0002"
    assert exc.value.expected_version == -1
    assert exc.value.actual_version == 1


@pytest.mark.asyncio
async def test_double_decision_occ_collision_exactly_one_wins():
    store = InMemoryEventStore()
    stream_id = "loan-APEX-0099"

    await store.append(stream_id, [_event("ApplicationSubmitted")], expected_version=-1)
    await store.append(stream_id, [_event("CreditAnalysisRequested")], expected_version=1)
    await store.append(stream_id, [_event("FraudScreeningRequested")], expected_version=2)

    async def writer(agent_id: str):
        return await store.append(
            stream_id,
            [_event("CreditAnalysisCompleted", agent_id=agent_id, score=0.91)],
            expected_version=3,
            correlation_id=f"corr-{agent_id}",
        )

    results = await asyncio.gather(writer("A"), writer("B"), return_exceptions=True)

    successes = [r for r in results if isinstance(r, int)]
    failures = [r for r in results if isinstance(r, OptimisticConcurrencyError)]

    events = await store.load_stream(stream_id)
    winning = [e for e in events if e.event_type == "CreditAnalysisCompleted"]

    assert len(events) == 4
    assert len(winning) == 1 and winning[0].stream_position == 4
    assert len(successes) == 1 and successes[0] == 4
    assert len(failures) == 1
    assert failures[0].expected_version == 3
    assert failures[0].actual_version == 4


@pytest.mark.asyncio
async def test_append_writes_outbox_entry_for_each_event():
    store = InMemoryEventStore()
    await store.append(
        "loan-APEX-0010",
        [_event("ApplicationSubmitted"), _event("CreditAnalysisRequested")],
        expected_version=-1,
    )
    assert store.outbox_size == 2


@pytest.mark.asyncio
async def test_append_writes_outbox_payload_with_event_context():
    store = InMemoryEventStore()
    await store.append(
        "loan-APEX-0011",
        [_event("ApplicationSubmitted", application_id="APEX-0011")],
        expected_version=-1,
        correlation_id="corr-0011",
        causation_id="cause-0011",
    )

    outbox_entry = store._outbox[0]
    payload = outbox_entry["payload"]
    assert outbox_entry["destination"] == "projection_daemon"
    assert payload["stream_id"] == "loan-APEX-0011"
    assert payload["stream_position"] == 1
    assert payload["global_position"] == 1
    assert payload["event_type"] == "ApplicationSubmitted"
    assert payload["payload"]["application_id"] == "APEX-0011"
    assert payload["metadata"]["correlation_id"] == "corr-0011"
    assert payload["metadata"]["causation_id"] == "cause-0011"


@pytest.mark.asyncio
async def test_append_zero_events_keeps_version_and_does_not_create_stream():
    store = InMemoryEventStore()
    v0 = await store.append("loan-APEX-0012", [], expected_version=-1)
    assert v0 == -1
    assert await store.stream_version("loan-APEX-0012") == -1
    with pytest.raises(KeyError):
        await store.get_stream_metadata("loan-APEX-0012")


@pytest.mark.asyncio
async def test_append_on_archived_stream_raises_occ_and_writes_no_event():
    store = InMemoryEventStore()
    stream_id = "loan-APEX-0013"
    await store.append(stream_id, [_event("ApplicationSubmitted")], expected_version=-1)
    await store.archive_stream(stream_id)

    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append(stream_id, [_event("CreditAnalysisRequested")], expected_version=1)

    assert exc.value.expected_version == 1
    assert exc.value.actual_version == 1
    events = await store.load_stream(stream_id)
    assert len(events) == 1
    assert store.outbox_size == 1


@pytest.mark.asyncio
async def test_append_on_archived_stream_wrong_expected_version_reports_current_version():
    store = InMemoryEventStore()
    stream_id = "loan-APEX-0014"
    await store.append(stream_id, [_event("ApplicationSubmitted")], expected_version=-1)
    await store.archive_stream(stream_id)

    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append(stream_id, [_event("CreditAnalysisRequested")], expected_version=99)

    assert exc.value.expected_version == 99
    assert exc.value.actual_version == 1


@pytest.mark.asyncio
async def test_double_archive_still_blocks_append():
    store = InMemoryEventStore()
    stream_id = "loan-APEX-0015"
    await store.append(stream_id, [_event("ApplicationSubmitted")], expected_version=-1)
    await store.archive_stream(stream_id)
    await store.archive_stream(stream_id)

    meta = await store.get_stream_metadata(stream_id)
    assert meta.archived_at is not None

    with pytest.raises(OptimisticConcurrencyError):
        await store.append(stream_id, [_event("CreditAnalysisRequested")], expected_version=1)


@pytest.mark.asyncio
async def test_outbox_starts_unpublished_with_zero_attempts():
    store = InMemoryEventStore()
    await store.append("loan-APEX-0020", [_event("ApplicationSubmitted")], expected_version=-1)
    pending = store.outbox_unpublished()
    assert len(pending) == 1
    assert pending[0]["published_at"] is None
    assert pending[0]["attempts"] == 0


@pytest.mark.asyncio
async def test_outbox_publish_failure_increments_attempts_stays_unpublished():
    store = InMemoryEventStore()
    await store.append("loan-APEX-0021", [_event("ApplicationSubmitted")], expected_version=-1)
    event_id = store._outbox[0]["event_id"]

    await store.outbox_mark_publish_failed(event_id)
    await store.outbox_mark_publish_failed(event_id)

    row = store._outbox[0]
    assert row["attempts"] == 2
    assert row["published_at"] is None
    assert len(store.outbox_unpublished()) == 1


@pytest.mark.asyncio
async def test_outbox_publish_success_sets_published_at():
    store = InMemoryEventStore()
    await store.append("loan-APEX-0022", [_event("ApplicationSubmitted")], expected_version=-1)
    event_id = store._outbox[0]["event_id"]

    await store.outbox_mark_published(event_id)

    row = store._outbox[0]
    assert row["published_at"] is not None
    assert store.outbox_unpublished() == []


@pytest.mark.asyncio
async def test_outbox_retry_does_not_duplicate_stream_events():
    store = InMemoryEventStore()
    stream_id = "loan-APEX-0023"
    await store.append(stream_id, [_event("ApplicationSubmitted")], expected_version=-1)
    event_id = store._outbox[0]["event_id"]

    await store.outbox_mark_publish_failed(event_id)
    await store.outbox_mark_publish_failed(event_id)

    events = await store.load_stream(stream_id)
    assert len(events) == 1
    assert store.outbox_size == 1


@pytest.mark.asyncio
async def test_outbox_mark_publish_failed_unknown_event_id_raises():
    store = InMemoryEventStore()
    from uuid import uuid4

    with pytest.raises(KeyError):
        await store.outbox_mark_publish_failed(uuid4())


@pytest.mark.asyncio
async def test_load_stream_empty_unknown_stream():
    store = InMemoryEventStore()
    assert await store.load_stream("loan-never-created") == []


@pytest.mark.asyncio
async def test_load_stream_empty_when_from_position_past_tail():
    store = InMemoryEventStore()
    await store.append("loan-tail", [_event("ApplicationSubmitted")], expected_version=-1)
    assert await store.load_stream("loan-tail", from_position=99) == []


@pytest.mark.asyncio
async def test_load_stream_empty_range_to_position_before_from():
    store = InMemoryEventStore()
    await store.append("loan-range", [_event("A"), _event("B")], expected_version=-1)
    assert await store.load_stream("loan-range", from_position=2, to_position=1) == []


@pytest.mark.asyncio
async def test_load_all_empty_store_yields_nothing():
    store = InMemoryEventStore()
    got = [e async for e in store.load_all(from_global_position=0)]
    assert got == []


@pytest.mark.asyncio
async def test_load_all_very_large_batch_size_returns_all_events():
    store = InMemoryEventStore()
    n = 120
    events = [_event("BatchEvt", i=i) for i in range(n)]
    await store.append("loan-big-batch", events, expected_version=-1)
    replay = [e async for e in store.load_all(from_global_position=0, batch_size=50_000)]
    assert len(replay) == n
    assert replay[0].global_position == 1
    assert replay[-1].global_position == n


@pytest.mark.asyncio
async def test_load_all_tiny_batch_size_still_exhausts_stream():
    store = InMemoryEventStore()
    n = 25
    events = [_event("TinyBatch", i=i) for i in range(n)]
    await store.append("loan-tiny-batch", events, expected_version=-1)
    replay = [e async for e in store.load_all(from_global_position=0, batch_size=1)]
    assert len(replay) == n


@pytest.mark.asyncio
async def test_outbox_can_fanout_to_multiple_destinations():
    store = InMemoryEventStore()
    await store.append(
        "loan-APEX-0090",
        [_event("ApplicationSubmitted")],
        expected_version=-1,
        outbox_destinations=["kafka:loan-events", "webhook:compliance-api"],
    )
    assert store.outbox_size == 2
    destinations = sorted(row["destination"] for row in store._outbox)
    assert destinations == ["kafka:loan-events", "webhook:compliance-api"]


@pytest.mark.asyncio
async def test_outbox_claim_batch_marks_attempts_once():
    store = InMemoryEventStore()
    await store.append("loan-APEX-0091", [_event("A"), _event("B")], expected_version=-1)
    claimed = await store.claim_outbox_batch(limit=2, destination="projection_daemon")
    assert len(claimed) == 2
    assert all(row["attempts"] == 1 for row in claimed)


@pytest.mark.asyncio
async def test_snapshot_version_mismatch_returns_none_and_keeps_replay_path():
    store = InMemoryEventStore()
    stream_id = "loan-APEX-0092"
    await store.append(stream_id, [_event("ApplicationSubmitted")], expected_version=-1)
    await store.save_snapshot(
        stream_id=stream_id,
        stream_position=1,
        aggregate_type="loan",
        snapshot_version=1,
        state={"state": "SUBMITTED"},
    )
    stale = await store.load_latest_snapshot(stream_id, current_snapshot_version=2)
    assert stale is None
    events = await store.load_stream(stream_id)
    assert len(events) == 1
