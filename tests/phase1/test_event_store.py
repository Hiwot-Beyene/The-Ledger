"""PostgreSQL EventStore integration tests.

Skip automatically if the server is down or the DB URL is wrong (``pytest.skip``), so CI/local
runs without Postgres stay green. With Postgres up, applies ``src/schema.sql`` if needed and
truncates tables around each test.

URL: ``tests/support/postgres_support.get_postgres_test_url``.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from event_store import EventStore, OptimisticConcurrencyError
from tests.support.postgres_support import (
    ensure_event_store_schema,
    get_postgres_test_url,
    redact_url,
    truncate_event_store_tables,
)

pytestmark = [pytest.mark.postgres_integration]


@pytest.fixture
async def store() -> EventStore:
    url = get_postgres_test_url()
    es = EventStore(url)
    try:
        await asyncio.wait_for(es.connect(), timeout=float(os.environ.get("POSTGRES_CONNECT_TIMEOUT", "8")))
    except Exception as exc:
        pytest.skip(
            "PostgreSQL not available: {!r}. URL={!r}. "
            "See docs/local_postgres_testing.md".format(exc, redact_url(url))
        )
    pool = es._require_pool()
    await ensure_event_store_schema(pool)
    await truncate_event_store_tables(pool)
    yield es
    await truncate_event_store_tables(pool)
    await es.close()


def _event(etype: str, n: int = 1) -> list[dict]:
    return [
        {"event_type": etype, "event_version": 1, "payload": {"seq": i, "test": True}}
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_append_new_stream(store: EventStore) -> None:
    version = await store.append("test-new-001", _event("TestEvent"), expected_version=-1)
    assert version == 1


@pytest.mark.asyncio
async def test_append_existing_stream(store: EventStore) -> None:
    await store.append("test-exist-001", _event("TestEvent"), expected_version=-1)
    version = await store.append("test-exist-001", _event("TestEvent2"), expected_version=1)
    assert version == 2


@pytest.mark.asyncio
async def test_append_empty_list_returns_stream_version_without_writes(store: EventStore) -> None:
    await store.append("test-empty-append", _event("Seed"), expected_version=-1)
    v = await store.append("test-empty-append", [], expected_version=1)
    assert v == 1
    events = await store.load_stream("test-empty-append")
    assert len(events) == 1


@pytest.mark.asyncio
async def test_occ_wrong_version_raises(store: EventStore) -> None:
    await store.append("test-occ-001", _event("E"), expected_version=-1)
    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append("test-occ-001", _event("E"), expected_version=99)
    assert exc.value.expected == 99
    assert exc.value.actual == 1


@pytest.mark.asyncio
async def test_concurrent_double_append_exactly_one_succeeds(store: EventStore) -> None:
    await store.append("test-concurrent-001", _event("Init"), expected_version=-1)
    results = await asyncio.gather(
        store.append("test-concurrent-001", _event("A"), expected_version=1),
        store.append("test-concurrent-001", _event("B"), expected_version=1),
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, int)]
    errors = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    assert len(successes) == 1, f"Expected exactly 1 success, got {len(successes)}"
    assert len(errors) == 1


@pytest.mark.asyncio
async def test_concurrent_five_appends_exactly_one_succeeds(store: EventStore) -> None:
    sid = "test-concurrent-five"
    await store.append(sid, _event("Init"), expected_version=-1)
    results = await asyncio.gather(
        *[store.append(sid, _event(f"E{i}"), expected_version=1) for i in range(5)],
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, int)]
    occ = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    assert len(successes) == 1
    assert len(occ) == 4


@pytest.mark.asyncio
async def test_load_stream_ordered(store: EventStore) -> None:
    await store.append("test-load-001", _event("E", 3), expected_version=-1)
    events = await store.load_stream("test-load-001")
    assert len(events) == 3
    positions = [e["stream_position"] for e in events]
    assert positions == sorted(positions)


@pytest.mark.asyncio
async def test_load_stream_unknown_stream_empty(store: EventStore) -> None:
    assert await store.load_stream("loan-never-created-integration") == []


@pytest.mark.asyncio
async def test_load_stream_from_position_past_tail_empty(store: EventStore) -> None:
    await store.append("loan-tail-pg", _event("ApplicationSubmitted"), expected_version=-1)
    assert await store.load_stream("loan-tail-pg", from_position=99) == []


@pytest.mark.asyncio
async def test_load_stream_range_to_before_from_empty(store: EventStore) -> None:
    await store.append("loan-range-pg", _event("A", 2), expected_version=-1)
    assert await store.load_stream("loan-range-pg", from_position=2, to_position=1) == []


@pytest.mark.asyncio
async def test_stream_version(store: EventStore) -> None:
    await store.append("test-ver-001", _event("E", 4), expected_version=-1)
    assert await store.stream_version("test-ver-001") == 4


@pytest.mark.asyncio
async def test_stream_version_nonexistent(store: EventStore) -> None:
    assert await store.stream_version("test-does-not-exist") == -1


@pytest.mark.asyncio
async def test_load_all_yields_in_global_order(store: EventStore) -> None:
    await store.append("test-global-A", _event("E", 2), expected_version=-1)
    await store.append("test-global-B", _event("E", 2), expected_version=-1)
    all_events = [e async for e in store.load_all(from_global_position=0)]
    positions = [e["global_position"] for e in all_events]
    assert positions == sorted(positions)


@pytest.mark.asyncio
async def test_load_all_empty_store_yields_nothing(store: EventStore) -> None:
    got = [e async for e in store.load_all(from_global_position=0)]
    assert got == []


@pytest.mark.asyncio
async def test_load_all_very_large_batch_size_returns_all_events(store: EventStore) -> None:
    n = 120
    await store.append("loan-big-batch-pg", _event("BatchEvt", n), expected_version=-1)
    replay = [e async for e in store.load_all(from_global_position=0, batch_size=50_000)]
    assert len(replay) == n
    assert replay[0].global_position == 1
    assert replay[-1].global_position == n


@pytest.mark.asyncio
async def test_load_all_tiny_batch_size_exhausts_global_stream(store: EventStore) -> None:
    n = 25
    await store.append("loan-tiny-batch-pg", _event("TinyBatch", n), expected_version=-1)
    replay = [e async for e in store.load_all(from_global_position=0, batch_size=1)]
    assert len(replay) == n


@pytest.mark.asyncio
async def test_load_all_event_types_filter(store: EventStore) -> None:
    await store.append("s-a", _event("TypeAlpha"), expected_version=-1)
    await store.append("s-b", _event("TypeBeta"), expected_version=-1)
    await store.append("s-c", _event("TypeAlpha"), expected_version=-1)
    got = [e async for e in store.load_all(from_global_position=0, event_types=["TypeBeta"])]
    assert len(got) == 1
    assert got[0].event_type == "TypeBeta"


@pytest.mark.asyncio
async def test_load_all_event_types_no_match_yields_empty(store: EventStore) -> None:
    await store.append("s-only-alpha", _event("OnlyAlpha"), expected_version=-1)
    got = [e async for e in store.load_all(from_global_position=0, event_types=["MissingType"])]
    assert got == []


@pytest.mark.asyncio
async def test_load_all_rejects_invalid_batch_size(store: EventStore) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        async for _ in store.load_all(batch_size=0):
            pass


@pytest.mark.asyncio
async def test_append_to_archived_stream_raises_occ(store: EventStore) -> None:
    stream_id = "test-arch-neg-001"
    await store.append(stream_id, _event("Init"), expected_version=-1)
    await store.archive_stream(stream_id)
    with pytest.raises(OptimisticConcurrencyError) as exc:
        await store.append(stream_id, _event("AfterArchive"), expected_version=1)
    assert exc.value.expected == 1
    assert exc.value.actual == 1
    events = await store.load_stream(stream_id)
    assert len(events) == 1


@pytest.mark.asyncio
async def test_outbox_row_inserted_with_append_for_retry_semantics(store: EventStore) -> None:
    stream_id = "test-outbox-neg-001"
    await store.append(stream_id, _event("E"), expected_version=-1)
    pool = store._require_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT published_at, attempts, destination
            FROM outbox o
            JOIN events e ON e.event_id = o.event_id
            WHERE e.stream_id = $1
            """,
            stream_id,
        )
    assert row is not None
    assert row["published_at"] is None
    assert row["attempts"] == 0
    assert row["destination"] == "projection_daemon"

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE outbox SET attempts = attempts + 1 WHERE event_id = (SELECT event_id FROM events WHERE stream_id = $1 LIMIT 1)",
            stream_id,
        )
        row2 = await conn.fetchrow(
            "SELECT attempts FROM outbox o JOIN events e ON e.event_id = o.event_id WHERE e.stream_id = $1",
            stream_id,
        )
    assert row2["attempts"] == 1


@pytest.mark.asyncio
async def test_outbox_claim_and_publish_path(store: EventStore) -> None:
    stream_id = "test-outbox-claim-001"
    await store.append(
        stream_id,
        _event("E"),
        expected_version=-1,
        outbox_destinations=["kafka:loan-events", "webhook:compliance-api"],
    )
    claimed = await store.claim_outbox_batch(limit=10, destination="kafka:loan-events")
    assert len(claimed) == 1
    assert claimed[0]["destination"] == "kafka:loan-events"
    assert claimed[0]["attempts"] == 1
    await store.outbox_mark_published(claimed[0]["id"])


@pytest.mark.asyncio
async def test_double_decision_occ_collision_postgres(store: EventStore) -> None:
    stream_id = "test-double-decision-pg"
    await store.append(stream_id, _event("Init"), expected_version=-1)
    await store.append(stream_id, _event("E2"), expected_version=1)
    await store.append(stream_id, _event("E3"), expected_version=2)

    async def writer(tag: str):
        return await store.append(stream_id, _event("CreditAnalysisCompleted", tag=tag), expected_version=3)

    results = await asyncio.gather(writer("A"), writer("B"), return_exceptions=True)
    events = await store.load_stream(stream_id)
    winning = [e for e in events if e.event_type == "CreditAnalysisCompleted"]
    successes = [r for r in results if isinstance(r, int)]
    failures = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    assert len(events) == 4
    assert len(winning) == 1 and winning[0].stream_position == 4
    assert len(successes) == 1 and successes[0] == 4
    assert len(failures) == 1
    assert failures[0].expected_version == 3
    assert failures[0].actual_version == 4


@pytest.mark.asyncio
async def test_concurrent_many_streams_replay_consistent(store: EventStore) -> None:
    n_streams = 15
    per_stream = 4

    async def fill(i: int) -> tuple[str, int]:
        sid = f"test-conc-load-{i}"
        v = await store.append(sid, _event("Init"), expected_version=-1)
        for _ in range(per_stream - 1):
            v = await store.append(sid, _event("More"), expected_version=v)
        return sid, v

    rows = await asyncio.gather(*[fill(i) for i in range(n_streams)])
    assert len(rows) == n_streams
    assert all(v == per_stream for _, v in rows)
    replay = [e async for e in store.load_all(from_global_position=0)]
    assert len(replay) == n_streams * per_stream
    gps = [e.global_position for e in replay]
    assert gps == sorted(gps) and gps[0] == 1 and gps[-1] == len(gps)


@pytest.mark.asyncio
async def test_projection_checkpoint_upsert(store: EventStore) -> None:
    assert await store.load_checkpoint("integration-proj") == 0
    await store.save_checkpoint("integration-proj", 10)
    assert await store.load_checkpoint("integration-proj") == 10
    await store.save_checkpoint("integration-proj", 2500)
    assert await store.load_checkpoint("integration-proj") == 2500


@pytest.mark.asyncio
async def test_snapshot_roundtrip_and_version_guard(store: EventStore) -> None:
    stream_id = "test-snapshot-001"
    await store.append(stream_id, _event("Init"), expected_version=-1)
    sid = await store.save_snapshot(
        stream_id=stream_id,
        stream_position=1,
        aggregate_type="loan",
        snapshot_version=1,
        state={"state": "SUBMITTED"},
    )
    assert sid is not None
    snap = await store.load_latest_snapshot(stream_id, current_snapshot_version=1)
    assert snap is not None
    assert snap["stream_position"] == 1
    stale = await store.load_latest_snapshot(stream_id, current_snapshot_version=2)
    assert stale is None
