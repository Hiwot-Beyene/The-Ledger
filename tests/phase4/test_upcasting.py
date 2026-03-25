from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime

import pytest

from event_store import EventStore, InMemoryEventStore
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


@pytest.mark.asyncio
async def test_upcast_is_read_time_only_and_does_not_mutate_stored_row(store: EventStore) -> None:
    stream_id = "loan-upcast-proof-001"
    v1_event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 1,
        "payload": {
            "application_id": "app-1",
            "session_id": "sess-1",
            "completed_at": datetime(2024, 10, 10, tzinfo=UTC).isoformat(),
        },
    }
    await store.append(stream_id, [v1_event], expected_version=-1)

    pool = store._require_pool()
    async with pool.acquire() as conn:
        before = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE stream_id = $1 ORDER BY stream_position LIMIT 1",
            stream_id,
        )
    assert before is not None
    assert int(before["event_version"]) == 1
    raw_before = dict(before["payload"])

    loaded = await store.load_stream(stream_id)
    assert loaded[0].event_version == 2
    assert "model_version" in loaded[0].payload
    assert "confidence_score" in loaded[0].payload
    assert loaded[0].payload["confidence_score"] is None
    assert "regulatory_basis" in loaded[0].payload

    async with pool.acquire() as conn:
        after = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE stream_id = $1 ORDER BY stream_position LIMIT 1",
            stream_id,
        )
    assert after is not None
    assert int(after["event_version"]) == 1
    assert dict(after["payload"]) == raw_before


@pytest.mark.asyncio
async def test_decision_upcast_reconstructs_model_versions_from_agent_context() -> None:
    store = InMemoryEventStore()
    await store.append(
        "agent-credit_analysis-sess-a",
        [
            {
                "event_type": "AgentContextLoaded",
                "event_version": 1,
                "payload": {
                    "session_id": "sess-a",
                    "agent_id": "credit_analysis",
                    "application_id": "app-1",
                    "context_source": "replay",
                    "model_version": "credit-v3",
                    "loaded_at": datetime.now(UTC).isoformat(),
                },
            }
        ],
        expected_version=-1,
    )
    await store.append(
        "loan-app-1",
        [
            {
                "event_type": "DecisionGenerated",
                "event_version": 1,
                "payload": {
                    "application_id": "app-1",
                    "recommendation": "APPROVE",
                    "executive_summary": "ok",
                    "generated_at": datetime.now(UTC).isoformat(),
                    "confidence_score": 0.8,
                    "contributing_agent_sessions": ["sess-a"],
                },
            }
        ],
        expected_version=-1,
    )
    loaded = await store.load_stream("loan-app-1")
    assert loaded[0].event_version == 2
    assert loaded[0].payload["model_versions"] == {"sess-a": "credit-v3"}
