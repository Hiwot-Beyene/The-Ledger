from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, AsyncIterator, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, TypeVar, runtime_checkable
from uuid import UUID, uuid4

import asyncpg
import asyncpg.exceptions as apg_exc

from models.events import (
    BaseEvent,
    OptimisticConcurrencyError,
    StoredEvent,
    StreamMetadata,
)
from upcasting.registry import UpcasterRegistry

T = TypeVar("T")

_TRANSIENT_ASYNCPG_TYPES: tuple[type[BaseException], ...] = (
    apg_exc.DeadlockDetectedError,
    apg_exc.SerializationError,
    apg_exc.ConnectionDoesNotExistError,
    apg_exc.ConnectionFailureError,
    apg_exc.CannotConnectNowError,
    apg_exc.CrashShutdownError,
    apg_exc.AdminShutdownError,
    apg_exc.TooManyConnectionsError,
)

_TRANSIENT_SQLSTATES = frozenset(
    {
        "40P01",
        "40001",
        "08006",
        "08003",
        "57P03",
        "57P01",
        "08001",
    }
)


def is_transient_db_error(exc: BaseException) -> bool:
    """True for errors where a bounded exponential backoff retry may succeed (Postgres / asyncpg)."""
    if isinstance(exc, OptimisticConcurrencyError):
        return False
    if isinstance(exc, _TRANSIENT_ASYNCPG_TYPES):
        return True
    if isinstance(exc, TimeoutError | asyncio.TimeoutError):
        return True
    if isinstance(exc, OSError):
        return exc.errno in (104, 110, 111, 32, 54)
    if isinstance(exc, asyncpg.PostgresError):
        st = getattr(exc, "sqlstate", None)
        if st in _TRANSIENT_SQLSTATES:
            return True
    return False


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def coerce_jsonb_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        s = value.strip()
        return json.loads(s) if s else {}
    if isinstance(value, (bytes, bytearray)):
        s = value.decode().strip()
        return json.loads(s) if s else {}
    return {}


@runtime_checkable
class EventStoreLike(Protocol):
    """Contract shared by ``EventStore`` and ``InMemoryEventStore`` (no ``connect``/``close``)."""

    async def stream_version(self, stream_id: str) -> int: ...

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent] | list[dict[str, Any]],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        **kwargs: Any,
    ) -> int: ...

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]: ...

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
        **kwargs: Any,
    ) -> AsyncIterator[StoredEvent]: ...

    async def archive_stream(self, stream_id: str) -> None: ...

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata: ...

    async def get_event(self, event_id: UUID) -> StoredEvent | None: ...

    async def claim_outbox_batch(
        self,
        limit: int = 100,
        destination: str | None = None,
    ) -> list[dict[str, Any]]: ...

    async def outbox_mark_published(self, outbox_id: UUID) -> None: ...

    async def outbox_mark_publish_failed(self, outbox_id: UUID) -> None: ...

    async def save_snapshot(
        self,
        stream_id: str,
        stream_position: int,
        aggregate_type: str,
        snapshot_version: int,
        state: dict[str, Any],
    ) -> UUID: ...

    async def load_latest_snapshot(
        self,
        stream_id: str,
        current_snapshot_version: int | None = None,
    ) -> dict[str, Any] | None: ...

    async def save_checkpoint(self, projection_name: str, position: int) -> None: ...

    async def load_checkpoint(self, projection_name: str) -> int: ...


def _to_store_event_dict(event: BaseEvent | dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, BaseEvent):
        return event.to_store_dict()
    if {"event_type", "payload"}.issubset(event):
        return {
            "event_type": event["event_type"],
            "event_version": int(event.get("event_version", 1)),
            "payload": dict(event.get("payload", {})),
        }
    raise TypeError("Each event must be a BaseEvent or an event dict with event_type/payload")


class EventStore:
    """PostgreSQL-backed event store. Transient asyncpg/Postgres errors use bounded exponential backoff
    (see ``db_retry_*``). Optimistic concurrency failures are never retried."""

    def __init__(
        self,
        db_url: str,
        upcaster_registry: UpcasterRegistry | None = None,
        *,
        db_retry_max_attempts: int = 3,
        db_retry_base_delay: float = 0.05,
        db_retry_max_delay: float = 2.0,
    ) -> None:
        self.db_url = db_url
        self.upcasters = upcaster_registry or UpcasterRegistry()
        self._pool: asyncpg.Pool | None = None
        self._db_retry_max_attempts = max(1, int(db_retry_max_attempts))
        self._db_retry_base_delay = float(db_retry_base_delay)
        self._db_retry_max_delay = float(db_retry_max_delay)

    async def _run_with_db_retry(self, op: Callable[[], Awaitable[T]]) -> T:
        if self._db_retry_max_attempts <= 1:
            return await op()
        last: Exception | None = None
        for attempt in range(self._db_retry_max_attempts):
            try:
                return await op()
            except Exception as e:
                last = e
                if attempt >= self._db_retry_max_attempts - 1 or not is_transient_db_error(e):
                    raise
                delay = min(
                    self._db_retry_max_delay,
                    self._db_retry_base_delay * (2**attempt),
                )
                await asyncio.sleep(delay)
        assert last is not None
        raise last

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=10)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def stream_version(self, stream_id: str) -> int:
        pool = self._require_pool()

        async def _read() -> int:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams WHERE stream_id = $1", stream_id
                )
                return -1 if row is None else int(row["current_version"])

        return await self._run_with_db_retry(_read)

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent] | list[dict[str, Any]],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        **kwargs: Any,
    ) -> int:
        if not events:
            return await self.stream_version(stream_id)

        pool = self._require_pool()
        aggregate_type = stream_id.split("-", 1)[0]
        prepared_events = [_to_store_event_dict(event) for event in events]
        call_metadata = kwargs.get("metadata") if isinstance(kwargs.get("metadata"), dict) else {}
        outbox_destinations = kwargs.get("outbox_destinations")
        if isinstance(outbox_destinations, str):
            outbox_destinations = [outbox_destinations]
        if not outbox_destinations:
            outbox_destinations = ["projection_daemon"]
        metadata = {
            "correlation_id": correlation_id,
            "causation_id": causation_id,
            **call_metadata,
        }
        metadata = {k: v for k, v in metadata.items() if v is not None}

        expected_db_version = 0 if expected_version == -1 else expected_version

        async def _append_tx() -> int:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    if expected_version == -1:
                        await conn.execute(
                            """
                            INSERT INTO event_streams(stream_id, aggregate_type, current_version, metadata)
                            VALUES($1, $2, 0, '{}'::jsonb)
                            ON CONFLICT (stream_id) DO NOTHING
                            """,
                            stream_id,
                            aggregate_type,
                        )

                    cas_row = await conn.fetchrow(
                        """
                        UPDATE event_streams
                        SET current_version = current_version + $2
                        WHERE stream_id = $1 AND current_version = $3 AND archived_at IS NULL
                        RETURNING current_version
                        """,
                        stream_id,
                        len(prepared_events),
                        expected_db_version,
                    )
                    if cas_row is None:
                        actual_row = await conn.fetchrow(
                            "SELECT current_version FROM event_streams WHERE stream_id = $1",
                            stream_id,
                        )
                        actual = -1 if actual_row is None else int(actual_row["current_version"])
                        raise OptimisticConcurrencyError(stream_id, expected_version, actual)

                    new_version = int(cas_row["current_version"])
                    first_position = expected_db_version + 1

                    for idx, event in enumerate(prepared_events):
                        stream_position = first_position + idx
                        event_row = await conn.fetchrow(
                            """
                            INSERT INTO events (
                                stream_id, stream_position, event_type, event_version, payload, metadata
                            )
                            VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
                            RETURNING event_id, global_position
                            """,
                            stream_id,
                            stream_position,
                            event["event_type"],
                            int(event.get("event_version", 1)),
                            json.dumps(event.get("payload", {}), default=_json_default),
                            json.dumps(metadata),
                        )
                        outbox_payload = {
                            "event_id": str(event_row["event_id"]),
                            "stream_id": stream_id,
                            "stream_position": stream_position,
                            "event_type": event["event_type"],
                            "event_version": int(event.get("event_version", 1)),
                            "payload": event.get("payload", {}),
                            "metadata": metadata,
                            "global_position": int(event_row["global_position"]),
                        }
                        for destination in outbox_destinations:
                            await conn.execute(
                                """
                                INSERT INTO outbox(event_id, destination, payload)
                                VALUES ($1, $2, $3::jsonb)
                                """,
                                event_row["event_id"],
                                destination,
                                json.dumps(outbox_payload, default=_json_default),
                            )

                    return new_version

        return await self._run_with_db_retry(_append_tx)

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        pool = self._require_pool()

        async def _load() -> list[StoredEvent]:
            async with pool.acquire() as conn:
                if to_position is None:
                    rows = await conn.fetch(
                        """
                        SELECT event_id, stream_id, stream_position, global_position, event_type,
                               event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE stream_id = $1 AND stream_position >= $2
                        ORDER BY stream_position ASC
                        """,
                        stream_id,
                        from_position,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT event_id, stream_id, stream_position, global_position, event_type,
                               event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE stream_id = $1 AND stream_position >= $2 AND stream_position <= $3
                        ORDER BY stream_position ASC
                        """,
                        stream_id,
                        from_position,
                        to_position,
                    )
            out: list[StoredEvent] = []
            for row in rows:
                raw = self._raw_stored_event_from_row(row)
                out.append(await self._finalize_read_event(raw, owning_stream_id=stream_id))
            return out

        return await self._run_with_db_retry(_load)

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
        **kwargs: Any,
    ) -> AsyncIterator[StoredEvent]:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if "from_position" in kwargs and isinstance(kwargs["from_position"], int):
            from_global_position = kwargs["from_position"]
        pool = self._require_pool()
        last_position = from_global_position
        async with pool.acquire() as conn:
            while True:
                lp = last_position

                async def fetch_page() -> list[asyncpg.Record]:
                    if event_types:
                        return await conn.fetch(
                            """
                            SELECT event_id, stream_id, stream_position, global_position, event_type,
                                   event_version, payload, metadata, recorded_at
                            FROM events
                            WHERE global_position > $1 AND event_type = ANY($2::text[])
                            ORDER BY global_position ASC
                            LIMIT $3
                            """,
                            lp,
                            event_types,
                            batch_size,
                        )
                    return await conn.fetch(
                        """
                        SELECT event_id, stream_id, stream_position, global_position, event_type,
                               event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > $1
                        ORDER BY global_position ASC
                        LIMIT $2
                        """,
                        lp,
                        batch_size,
                    )

                rows = await self._run_with_db_retry(fetch_page)

                if not rows:
                    break

                for row in rows:
                    raw = self._raw_stored_event_from_row(row)
                    stored = await self._finalize_read_event(
                        raw, owning_stream_id=str(row["stream_id"])
                    )
                    last_position = stored.global_position
                    yield stored

                if len(rows) < batch_size:
                    break

    async def archive_stream(self, stream_id: str) -> None:
        pool = self._require_pool()

        async def _archive() -> None:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE event_streams
                    SET archived_at = NOW()
                    WHERE stream_id = $1 AND archived_at IS NULL
                    """,
                    stream_id,
                )
                if result.endswith("0"):
                    exists = await conn.fetchrow(
                        "SELECT 1 FROM event_streams WHERE stream_id = $1", stream_id
                    )
                    if exists is None:
                        raise KeyError(f"Stream not found: {stream_id}")

        await self._run_with_db_retry(_archive)

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        pool = self._require_pool()

        async def _meta() -> StreamMetadata:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT stream_id, aggregate_type, current_version, created_at, archived_at, metadata
                    FROM event_streams
                    WHERE stream_id = $1
                    """,
                    stream_id,
                )
                if row is None:
                    raise KeyError(f"Stream not found: {stream_id}")
                return StreamMetadata(
                    stream_id=row["stream_id"],
                    aggregate_type=row["aggregate_type"],
                    current_version=int(row["current_version"]),
                    created_at=row["created_at"],
                    archived_at=row["archived_at"],
                    metadata=coerce_jsonb_dict(row["metadata"]),
                )

        return await self._run_with_db_retry(_meta)

    async def get_event(self, event_id: UUID) -> StoredEvent | None:
        pool = self._require_pool()

        async def _one() -> StoredEvent | None:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT event_id, stream_id, stream_position, global_position, event_type,
                           event_version, payload, metadata, recorded_at
                    FROM events WHERE event_id = $1
                    """,
                    event_id,
                )
                if row is None:
                    return None
                raw = self._raw_stored_event_from_row(row)
                return await self._finalize_read_event(raw, owning_stream_id=str(row["stream_id"]))

        return await self._run_with_db_retry(_one)

    async def claim_outbox_batch(
        self,
        limit: int = 100,
        destination: str | None = None,
    ) -> list[dict[str, Any]]:
        """Claim unpublished outbox rows for publishing (increments attempts once per claim)."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        pool = self._require_pool()

        async def _claim() -> list[dict[str, Any]]:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    if destination:
                        rows = await conn.fetch(
                            """
                            SELECT id
                            FROM outbox
                            WHERE published_at IS NULL AND destination = $1
                            ORDER BY created_at ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT $2
                            """,
                            destination,
                            limit,
                        )
                    else:
                        rows = await conn.fetch(
                            """
                            SELECT id
                            FROM outbox
                            WHERE published_at IS NULL
                            ORDER BY created_at ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT $1
                            """,
                            limit,
                        )
                    if not rows:
                        return []
                    ids = [row["id"] for row in rows]
                    claimed = await conn.fetch(
                        """
                        UPDATE outbox
                        SET attempts = attempts + 1
                        WHERE id = ANY($1::uuid[])
                        RETURNING id, event_id, destination, payload, created_at, published_at, attempts
                        """,
                        ids,
                    )
                    return [
                        {
                            "id": row["id"],
                            "event_id": row["event_id"],
                            "destination": row["destination"],
                            "payload": coerce_jsonb_dict(row["payload"]),
                            "created_at": row["created_at"],
                            "published_at": row["published_at"],
                            "attempts": int(row["attempts"]),
                        }
                        for row in claimed
                    ]

        return await self._run_with_db_retry(_claim)

    async def outbox_mark_published(self, outbox_id: UUID) -> None:
        pool = self._require_pool()

        async def _mark() -> None:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE outbox
                    SET published_at = NOW()
                    WHERE id = $1
                    """,
                    outbox_id,
                )
                if result.endswith("0"):
                    raise KeyError(f"outbox row not found: {outbox_id}")

        await self._run_with_db_retry(_mark)

    async def outbox_mark_publish_failed(self, outbox_id: UUID) -> None:
        pool = self._require_pool()

        async def _mark_failed() -> None:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE outbox
                    SET attempts = attempts + 1
                    WHERE id = $1
                    """,
                    outbox_id,
                )
                if result.endswith("0"):
                    raise KeyError(f"outbox row not found: {outbox_id}")

        await self._run_with_db_retry(_mark_failed)

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        pool = self._require_pool()

        async def _save() -> None:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                    VALUES ($1, $2, NOW())
                    ON CONFLICT (projection_name) DO UPDATE
                    SET last_position = EXCLUDED.last_position, updated_at = NOW()
                    """,
                    projection_name,
                    int(position),
                )

        await self._run_with_db_retry(_save)

    async def load_checkpoint(self, projection_name: str) -> int:
        pool = self._require_pool()

        async def _load() -> int:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
                    projection_name,
                )
                return 0 if row is None else int(row["last_position"])

        return await self._run_with_db_retry(_load)

    async def save_snapshot(
        self,
        stream_id: str,
        stream_position: int,
        aggregate_type: str,
        snapshot_version: int,
        state: dict[str, Any],
    ) -> UUID:
        pool = self._require_pool()

        async def _save() -> UUID:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO snapshots(
                        stream_id, stream_position, aggregate_type, snapshot_version, state
                    )
                    VALUES($1, $2, $3, $4, $5::jsonb)
                    RETURNING snapshot_id
                    """,
                    stream_id,
                    stream_position,
                    aggregate_type,
                    snapshot_version,
                    json.dumps(state, default=_json_default),
                )
                return row["snapshot_id"]

        return await self._run_with_db_retry(_save)

    async def load_latest_snapshot(
        self,
        stream_id: str,
        current_snapshot_version: int | None = None,
    ) -> dict[str, Any] | None:
        pool = self._require_pool()

        async def _load() -> dict[str, Any] | None:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT snapshot_id, stream_id, stream_position, aggregate_type, snapshot_version, state, created_at
                    FROM snapshots
                    WHERE stream_id = $1
                    ORDER BY stream_position DESC
                    LIMIT 1
                    """,
                    stream_id,
                )
                if row is None:
                    return None
                if (
                    current_snapshot_version is not None
                    and int(row["snapshot_version"]) != current_snapshot_version
                ):
                    return None
                return {
                    "snapshot_id": row["snapshot_id"],
                    "stream_id": row["stream_id"],
                    "stream_position": int(row["stream_position"]),
                    "aggregate_type": row["aggregate_type"],
                    "snapshot_version": int(row["snapshot_version"]),
                    "state": coerce_jsonb_dict(row["state"]),
                    "created_at": row["created_at"],
                }

        return await self._run_with_db_retry(_load)

    def _raw_stored_event_from_row(self, row: asyncpg.Record) -> StoredEvent:
        return StoredEvent(
            event_id=row["event_id"],
            stream_id=row["stream_id"],
            stream_position=int(row["stream_position"]),
            global_position=int(row["global_position"]),
            event_type=row["event_type"],
            event_version=int(row["event_version"]),
            payload=coerce_jsonb_dict(row["payload"]),
            metadata=coerce_jsonb_dict(row["metadata"]),
            recorded_at=row["recorded_at"],
        )

    async def _finalize_read_event(self, ev: StoredEvent, *, owning_stream_id: str) -> StoredEvent:
        data = ev.model_dump(mode="python")
        if (
            owning_stream_id.startswith("loan-")
            and data.get("event_type") == "DecisionGenerated"
            and int(data.get("event_version", 1)) == 1
        ):
            versions = await self._decision_model_versions_for_payload(data.get("payload") or {})
            data = self.upcasters.upcast(data, {"decision_model_versions": versions})
        else:
            data = self.upcasters.upcast(data, {})
        return StoredEvent(**data)

    async def _decision_model_versions_for_payload(self, payload: dict[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        for raw in payload.get("contributing_agent_sessions") or []:
            token = str(raw).strip()
            if not token:
                continue
            stream_ids = await self._resolve_agent_stream_ids(token)
            for sid in stream_ids:
                agent_events = await self.load_stream(sid)
                for ae in agent_events:
                    if ae.event_type == "AgentContextLoaded":
                        mv = ae.payload.get("model_version")
                        if mv:
                            out[token] = str(mv)
                        break
        return out

    async def _resolve_agent_stream_ids(self, token: str) -> list[str]:
        t = str(token).strip()
        if not t:
            return []
        if t.startswith("agent-"):
            return [t]
        pool = self._require_pool()

        async def _q() -> list[str]:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT stream_id FROM events
                    WHERE stream_id LIKE 'agent-%' AND stream_id LIKE $1
                    LIMIT 16
                    """,
                    f"%-{t}",
                )
                return [str(r["stream_id"]) for r in rows]

        return await self._run_with_db_retry(_q)

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("EventStore is not connected. Call connect() first.")
        return self._pool


class InMemoryEventStore:
    def __init__(self, upcaster_registry: UpcasterRegistry | None = None) -> None:
        self.upcasters = upcaster_registry or UpcasterRegistry()
        self._streams: dict[str, list[StoredEvent]] = {}
        self._versions: dict[str, int] = {}
        self._stream_metadata: dict[str, StreamMetadata] = {}
        self._global: list[StoredEvent] = []
        self._checkpoints: dict[str, int] = {}
        self._outbox: list[dict[str, Any]] = []
        self._snapshots: dict[str, list[dict[str, Any]]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent] | list[dict[str, Any]],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        **kwargs: Any,
    ) -> int:
        if not events:
            return await self.stream_version(stream_id)

        if "metadata" in kwargs and kwargs["metadata"] and isinstance(kwargs["metadata"], dict):
            if correlation_id is None:
                correlation_id = kwargs["metadata"].get("correlation_id")
            if causation_id is None:
                causation_id = kwargs["metadata"].get("causation_id")
        outbox_destinations = kwargs.get("outbox_destinations")
        if isinstance(outbox_destinations, str):
            outbox_destinations = [outbox_destinations]
        if not outbox_destinations:
            outbox_destinations = ["projection_daemon"]

        if stream_id not in self._locks:
            self._locks[stream_id] = asyncio.Lock()

        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            stream_meta = self._stream_metadata.get(stream_id)
            if stream_meta is not None and stream_meta.archived_at is not None:
                raise OptimisticConcurrencyError(stream_id, expected_version, stream_meta.current_version)

            prepared = [_to_store_event_dict(event) for event in events]
            metadata = {
                k: v
                for k, v in {
                    "correlation_id": correlation_id,
                    "causation_id": causation_id,
                }.items()
                if v is not None
            }

            aggregate_type = stream_id.split("-", 1)[0]
            if stream_id not in self._stream_metadata:
                self._stream_metadata[stream_id] = StreamMetadata(
                    stream_id=stream_id,
                    aggregate_type=aggregate_type,
                    current_version=0,
                    created_at=datetime.now(UTC),
                    archived_at=None,
                    metadata={},
                )

            expected_db_version = 0 if expected_version == -1 else expected_version
            first_position = expected_db_version + 1
            stream_events = self._streams.setdefault(stream_id, [])
            for idx, event in enumerate(prepared):
                event_id = uuid4()
                stored = StoredEvent(
                    event_id=event_id,
                    stream_id=stream_id,
                    stream_position=first_position + idx,
                    global_position=len(self._global) + 1,
                    event_type=event["event_type"],
                    event_version=int(event.get("event_version", 1)),
                    payload=dict(event.get("payload", {})),
                    metadata=dict(metadata),
                    recorded_at=datetime.now(UTC),
                )
                stream_events.append(stored)
                self._global.append(stored)
                for destination in outbox_destinations:
                    self._outbox.append(
                        {
                            "id": uuid4(),
                            "event_id": event_id,
                            "destination": destination,
                            "payload": {
                                "event_id": str(event_id),
                                "stream_id": stream_id,
                                "stream_position": stored.stream_position,
                                "global_position": stored.global_position,
                                "event_type": stored.event_type,
                                "event_version": stored.event_version,
                                "payload": dict(stored.payload),
                                "metadata": dict(stored.metadata),
                            },
                            "created_at": datetime.now(UTC),
                            "published_at": None,
                            "attempts": 0,
                        }
                    )

            new_version = expected_db_version + len(prepared)
            self._versions[stream_id] = new_version
            stream_meta = self._stream_metadata[stream_id]
            self._stream_metadata[stream_id] = StreamMetadata(
                stream_id=stream_meta.stream_id,
                aggregate_type=stream_meta.aggregate_type,
                current_version=new_version,
                created_at=stream_meta.created_at,
                archived_at=stream_meta.archived_at,
                metadata=stream_meta.metadata,
            )
            return new_version

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        stream_events = self._streams.get(stream_id, [])
        out: list[StoredEvent] = []
        for event in stream_events:
            if event.stream_position >= from_position and (
                to_position is None or event.stream_position <= to_position
            ):
                out.append(await self._finalize_read_event(event, owning_stream_id=stream_id))
        return out

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
        **kwargs: Any,
    ) -> AsyncIterator[StoredEvent]:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if "from_position" in kwargs and isinstance(kwargs["from_position"], int):
            from_global_position = kwargs["from_position"]
        for event in self._global:
            if event.global_position <= from_global_position:
                continue
            if event_types and event.event_type not in event_types:
                continue
            yield await self._finalize_read_event(event, owning_stream_id=event.stream_id)

    async def archive_stream(self, stream_id: str) -> None:
        if stream_id not in self._stream_metadata:
            raise KeyError(f"Stream not found: {stream_id}")
        meta = self._stream_metadata[stream_id]
        self._stream_metadata[stream_id] = StreamMetadata(
            stream_id=meta.stream_id,
            aggregate_type=meta.aggregate_type,
            current_version=meta.current_version,
            created_at=meta.created_at,
            archived_at=datetime.now(UTC),
            metadata=meta.metadata,
        )

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        if stream_id not in self._stream_metadata:
            raise KeyError(f"Stream not found: {stream_id}")
        return self._stream_metadata[stream_id]

    async def get_event(self, event_id: UUID | str) -> StoredEvent | None:
        event_id_str = str(event_id)
        for event in self._global:
            if str(event.event_id) == event_id_str:
                return await self._finalize_read_event(event, owning_stream_id=event.stream_id)
        return None

    async def _finalize_read_event(self, ev: StoredEvent, *, owning_stream_id: str) -> StoredEvent:
        data = ev.model_dump(mode="python")
        if (
            owning_stream_id.startswith("loan-")
            and data.get("event_type") == "DecisionGenerated"
            and int(data.get("event_version", 1)) == 1
        ):
            versions = await self._decision_model_versions_for_payload(data.get("payload") or {})
            data = self.upcasters.upcast(data, {"decision_model_versions": versions})
        else:
            data = self.upcasters.upcast(data, {})
        return StoredEvent(**data)

    async def _decision_model_versions_for_payload(self, payload: dict[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        for raw in payload.get("contributing_agent_sessions") or []:
            token = str(raw).strip()
            if not token:
                continue
            for sid in self._resolve_agent_stream_ids_memory(token):
                for ae in self._streams.get(sid, []):
                    if ae.event_type == "AgentContextLoaded":
                        mv = ae.payload.get("model_version")
                        if mv:
                            out[token] = str(mv)
                        break
        return out

    def _resolve_agent_stream_ids_memory(self, token: str) -> list[str]:
        t = str(token).strip()
        if not t:
            return []
        if t.startswith("agent-"):
            return [t]
        return [sid for sid in self._streams if sid.startswith("agent-") and sid.endswith(f"-{t}")]

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)

    @property
    def outbox_size(self) -> int:
        return len(self._outbox)

    def outbox_unpublished(self) -> list[dict[str, Any]]:
        return sorted(
            [row for row in self._outbox if row.get("published_at") is None],
            key=lambda r: r["created_at"],
        )

    async def claim_outbox_batch(
        self,
        limit: int = 100,
        destination: str | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        rows = self.outbox_unpublished()
        if destination:
            rows = [row for row in rows if row["destination"] == destination]
        claimed = rows[:limit]
        for row in claimed:
            row["attempts"] = int(row["attempts"]) + 1
        return claimed

    async def outbox_mark_publish_failed(self, row_id: UUID) -> None:
        for row in self._outbox:
            if row["id"] == row_id or row["event_id"] == row_id:
                row["attempts"] = int(row["attempts"]) + 1
                return
        raise KeyError(f"outbox row not found for id/event_id={row_id}")

    async def outbox_mark_published(self, row_id: UUID) -> None:
        for row in self._outbox:
            if row["id"] == row_id or row["event_id"] == row_id:
                row["published_at"] = datetime.now(UTC)
                return
        raise KeyError(f"outbox row not found for id/event_id={row_id}")

    async def save_snapshot(
        self,
        stream_id: str,
        stream_position: int,
        aggregate_type: str,
        snapshot_version: int,
        state: dict[str, Any],
    ) -> UUID:
        snapshot_id = uuid4()
        row = {
            "snapshot_id": snapshot_id,
            "stream_id": stream_id,
            "stream_position": int(stream_position),
            "aggregate_type": aggregate_type,
            "snapshot_version": int(snapshot_version),
            "state": dict(state),
            "created_at": datetime.now(UTC),
        }
        self._snapshots.setdefault(stream_id, []).append(row)
        self._snapshots[stream_id].sort(key=lambda s: s["stream_position"], reverse=True)
        return snapshot_id

    async def load_latest_snapshot(
        self,
        stream_id: str,
        current_snapshot_version: int | None = None,
    ) -> dict[str, Any] | None:
        rows = self._snapshots.get(stream_id, [])
        if not rows:
            return None
        latest = rows[0]
        if (
            current_snapshot_version is not None
            and int(latest["snapshot_version"]) != current_snapshot_version
        ):
            return None
        return dict(latest)
