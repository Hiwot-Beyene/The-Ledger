from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import os

import asyncpg
import pytest

from projections import (
    Projection,
    ProjectionDaemon,
    rebuild_compliance_audit_blue_green,
)
from models.events import StoredEvent


class _FakeStore:
    def __init__(self, events: list[StoredEvent]) -> None:
        self._events = sorted(events, key=lambda e: e.global_position)

    async def load_all(self, from_global_position: int = 0, batch_size: int = 500, **kwargs):
        emitted = 0
        for event in self._events:
            if event.global_position <= from_global_position:
                continue
            yield event
            emitted += 1
            if emitted >= batch_size:
                break


class _FakeProjection(Projection):
    name = "fake"
    subscribed_event_types = {"Wanted"}

    def __init__(self, store, fail_once_at: int | None = None) -> None:
        super().__init__(store)
        self.fail_once_at = fail_once_at
        self.handled: list[int] = []
        self.checkpoint = 0
        self._failed = False

    async def handle(self, event: StoredEvent) -> None:
        if self.fail_once_at == event.global_position and not self._failed:
            self._failed = True
            raise RuntimeError("boom once")
        self.handled.append(event.global_position)

    async def _handle_with_conn(self, conn: asyncpg.Connection, event: StoredEvent) -> None:
        await self.handle(event)

    async def get_checkpoint(self) -> int:
        return self.checkpoint

    async def update_checkpoint(self, position: int) -> None:
        self.checkpoint = max(self.checkpoint, int(position))

    async def get_lag(self) -> int:
        return 0

    async def apply_event_and_checkpoint(self, event: StoredEvent) -> None:
        await self.handle(event)
        await self.update_checkpoint(event.global_position)

    async def _reset_state(self) -> None:
        self.handled.clear()
        self.checkpoint = 0


def _event(global_position: int, event_type: str = "Wanted") -> StoredEvent:
    return StoredEvent(
        event_id=uuid4(),
        stream_id="loan-app-1",
        stream_position=global_position,
        global_position=global_position,
        event_type=event_type,
        event_version=1,
        payload={"application_id": "app-1"},
        metadata={},
        recorded_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_daemon_routes_only_subscribed_events():
    store = _FakeStore([_event(1, "Ignored"), _event(2, "Wanted"), _event(3, "Ignored")])
    proj = _FakeProjection(store)
    daemon = ProjectionDaemon(store, [proj], batch_size=100, max_retries=2)

    processed = await daemon._process_batch()

    assert processed == 3
    assert proj.handled == [2]
    assert proj.checkpoint == 2


@pytest.mark.asyncio
async def test_daemon_retries_and_advances_checkpoint():
    store = _FakeStore([_event(1, "Wanted")])
    proj = _FakeProjection(store, fail_once_at=1)
    daemon = ProjectionDaemon(store, [proj], batch_size=100, max_retries=3)

    processed = await daemon._process_batch()

    assert processed == 1
    assert proj.handled == [1]
    assert proj.checkpoint == 1
    assert daemon.get_metrics()["failures"] == 1


class _LaggyProjection(_FakeProjection):
    async def get_lag(self) -> int:
        return 50_000


@pytest.mark.asyncio
async def test_daemon_get_health_exposes_lag_slo():
    store = _FakeStore([])
    daemon = ProjectionDaemon(store, [_LaggyProjection(store)], batch_size=10, max_retries=1)
    health = await daemon.get_health(slo_max_lag_events=100)
    assert health["slo_projection_catchup_ok"] is False
    assert health["max_projection_lag"] == 50_000
    assert health["projection_lag_by_name"]["fake"] == 50_000


@pytest.mark.asyncio
async def test_rebuild_compliance_blue_green_requires_env_ack():
    class Dummy:
        pass

    with pytest.raises(RuntimeError, match="LEDGER_COMPLIANCE_REBUILD_DAEMON_PAUSED"):
        await rebuild_compliance_audit_blue_green(Dummy())  # type: ignore[arg-type]


@pytest.mark.postgres_integration
@pytest.mark.asyncio
async def test_sql_compliance_as_of_matches_projection_temporal() -> None:
    """Temporal ComplianceAuditView: snapshot boundary + replay; SqlProjectionQueries delegates here."""
    import asyncio
    import os

    from event_store import EventStore
    from mcp.server import SqlProjectionQueries
    from projections import ComplianceAuditViewProjection
    from models.events import StoredEvent
    from tests.support.postgres_support import (
        ensure_event_store_schema,
        get_postgres_test_url,
        redact_url,
        truncate_event_store_tables,
    )

    url = get_postgres_test_url()
    store = EventStore(url)
    try:
        await asyncio.wait_for(store.connect(), timeout=float(os.environ.get("POSTGRES_CONNECT_TIMEOUT", "8")))
    except Exception as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc!r} url={redact_url(url)!r}")

    pool = store._require_pool()
    await ensure_event_store_schema(pool)
    await truncate_event_store_tables(pool)
    try:
        app_id = "TEMP-COMP-1"
        t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2144, 6, 15, 9, 30, 0, tzinfo=UTC)
        p0 = json.dumps(
            {"application_id": app_id, "regulation_set_version": "v1", "rules_to_evaluate": ["R1"]}
        )
        p1 = json.dumps({"application_id": app_id, "rule_id": "R1"})
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO events(
                  event_id, stream_id, stream_position, event_type, event_version, payload, metadata, recorded_at
                ) VALUES
                (gen_random_uuid(), $1, 1, 'ComplianceCheckInitiated', 1, $2::jsonb, '{}'::jsonb, $3),
                (gen_random_uuid(), $1, 2, 'ComplianceRulePassed', 1, $4::jsonb, '{}'::jsonb, $5)
                """,
                f"compliance-{app_id}",
                p0,
                t0,
                p1,
                t1,
            )

        comp = ComplianceAuditViewProjection(store, snapshot_every_events=1)
        ev_rows = await pool.fetch(
            "SELECT * FROM events WHERE stream_id = $1 ORDER BY stream_position ASC",
            f"compliance-{app_id}",
        )
        for ev in ev_rows:
            se = StoredEvent(
                event_id=ev["event_id"],
                stream_id=ev["stream_id"],
                stream_position=int(ev["stream_position"]),
                global_position=int(ev["global_position"]),
                event_type=ev["event_type"],
                event_version=int(ev["event_version"]),
                payload=ev["payload"],
                metadata=ev["metadata"],
                recorded_at=ev["recorded_at"],
            )
            await comp.apply_event_and_checkpoint(se)

        sqlq = SqlProjectionQueries(store)
        as_of_mid = datetime(2100, 1, 1, tzinfo=UTC)
        direct = await ComplianceAuditViewProjection(store).get_compliance_at(app_id, as_of_mid)
        via_sql = await sqlq.get_compliance_as_of(app_id, as_of_mid)
        assert direct and via_sql
        assert direct.get("event_count") == via_sql.get("event_count") == 1
    finally:
        await truncate_event_store_tables(pool)
        await store.close()
