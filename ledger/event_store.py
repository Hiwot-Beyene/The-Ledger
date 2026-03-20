from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, AsyncIterator, TypeVar
from uuid import UUID, uuid4

import asyncpg
import asyncpg.exceptions as apg_exc

from ledger.schema.events import (
    BaseEvent,
    OptimisticConcurrencyError,
    StoredEvent,
    StreamMetadata,
)
from ledger.upcasters import UpcasterRegistry

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
                        await conn.execute(
                            """
                            INSERT INTO outbox(event_id, destination, payload)
                            VALUES ($1, $2, $3::jsonb)
                            """,
                            event_row["event_id"],
                            "projection_daemon",
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
            return [self._row_to_stored_event(row) for row in rows]

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
                    stored = self._row_to_stored_event(row)
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
                    metadata=dict(row["metadata"]),
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
                return self._row_to_stored_event(row)

        return await self._run_with_db_retry(_one)

    def _row_to_stored_event(self, row: asyncpg.Record) -> StoredEvent:
        event_dict = {
            "event_id": row["event_id"],
            "stream_id": row["stream_id"],
            "stream_position": int(row["stream_position"]),
            "global_position": int(row["global_position"]),
            "event_type": row["event_type"],
            "event_version": int(row["event_version"]),
            "payload": dict(row["payload"]),
            "metadata": dict(row["metadata"]),
            "recorded_at": row["recorded_at"],
        }
        event_dict = self.upcasters.upcast(event_dict)
        return StoredEvent(**event_dict)

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
                upcasted = StoredEvent(
                    **self.upcasters.upcast(stored.model_dump(mode="python"))
                )
                stream_events.append(upcasted)
                self._global.append(upcasted)
                self._outbox.append(
                    {
                        "event_id": event_id,
                        "destination": "projection_daemon",
                        "payload": {
                            "event_id": str(event_id),
                            "stream_id": stream_id,
                            "stream_position": upcasted.stream_position,
                            "global_position": upcasted.global_position,
                            "event_type": upcasted.event_type,
                            "event_version": upcasted.event_version,
                            "payload": dict(upcasted.payload),
                            "metadata": dict(upcasted.metadata),
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
        return [
            event
            for event in stream_events
            if event.stream_position >= from_position
            and (to_position is None or event.stream_position <= to_position)
        ]

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
        emitted = 0
        for event in self._global:
            if event.global_position <= from_global_position:
                continue
            if event_types and event.event_type not in event_types:
                continue
            yield event
            emitted += 1
            if emitted >= batch_size:
                emitted = 0

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
                return event
        return None

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)

    @property
    def outbox_size(self) -> int:
        return len(self._outbox)

    def outbox_unpublished(self) -> list[dict[str, Any]]:
        return [row for row in self._outbox if row.get("published_at") is None]

    async def outbox_mark_publish_failed(self, event_id: UUID) -> None:
        for row in self._outbox:
            if row["event_id"] == event_id:
                row["attempts"] = int(row["attempts"]) + 1
                return
        raise KeyError(f"outbox row not found for event_id={event_id}")

    async def outbox_mark_published(self, event_id: UUID) -> None:
        for row in self._outbox:
            if row["event_id"] == event_id:
                row["published_at"] = datetime.now(UTC)
                return
        raise KeyError(f"outbox row not found for event_id={event_id}")
