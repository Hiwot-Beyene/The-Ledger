from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

import asyncpg

from event_store import EventStore
from models.events import StoredEvent

from projections.util import _app_id, _dt, _event_time

class Projection(ABC):
    name: str
    subscribed_event_types: set[str]

    def __init__(self, store: EventStore) -> None:
        self._store = store

    @abstractmethod
    async def handle(self, event: StoredEvent) -> None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await self._handle_with_conn(conn, event)

    @abstractmethod
    async def _handle_with_conn(self, conn: asyncpg.Connection, event: StoredEvent) -> None:
        raise NotImplementedError

    async def get_checkpoint(self) -> int:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT last_position FROM projection_checkpoints
                WHERE projection_name = $1
                """,
                self.name,
            )
            return int(row["last_position"]) if row else 0

    async def update_checkpoint(self, position: int) -> None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            await self._update_checkpoint_with_conn(conn, position)

    async def _update_checkpoint_with_conn(
        self, conn: asyncpg.Connection, position: int
    ) -> None:
        await conn.execute(
            """
            INSERT INTO projection_checkpoints(projection_name, last_position, updated_at)
            VALUES($1, $2, NOW())
            ON CONFLICT (projection_name)
            DO UPDATE SET last_position = EXCLUDED.last_position, updated_at = NOW()
            """,
            self.name,
            int(position),
        )

    async def get_lag(self) -> int:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            latest = await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events")
            checkpoint = await self.get_checkpoint()
            return max(0, int(latest) - int(checkpoint))

    async def rebuild_from_scratch(self) -> None:
        await self._reset_state()
        await self.update_checkpoint(0)
        async for event in self._store.load_all(from_global_position=0, batch_size=500):
            if event.event_type in self.subscribed_event_types:
                await self.apply_event_and_checkpoint(event)

    async def apply_event_and_checkpoint(self, event: StoredEvent) -> None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await self._handle_with_conn(conn, event)
                await self._update_checkpoint_with_conn(conn, event.global_position)

    @abstractmethod
    async def _reset_state(self) -> None:
        raise NotImplementedError
