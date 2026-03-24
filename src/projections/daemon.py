from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from event_store import EventStore
from models.events import StoredEvent

from projections.base import Projection

logger = logging.getLogger("projections")


@dataclass(slots=True)
class DaemonMetrics:
    events_processed: int = 0
    batches_processed: int = 0
    failures: int = 0
    start_monotonic: float = 0.0

    @property
    def processed_events_per_sec(self) -> float:
        elapsed = max(0.001, time.monotonic() - self.start_monotonic)
        return self.events_processed / elapsed


class ProjectionDaemon:
    def __init__(
        self,
        store: EventStore,
        projections: list[Projection],
        *,
        batch_size: int = 250,
        max_retries: int = 3,
    ) -> None:
        self._store = store
        self._projections = {p.name: p for p in projections}
        self._event_map: dict[str, list[Projection]] = defaultdict(list)
        for p in projections:
            for event_type in p.subscribed_event_types:
                self._event_map[event_type].append(p)
        self._batch_size = max(1, int(batch_size))
        self._max_retries = max(1, int(max_retries))
        self._running = False
        self._stop_event = asyncio.Event()
        self._metrics = DaemonMetrics(start_monotonic=time.monotonic())

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        self._running = True
        self._stop_event.clear()
        while self._running:
            await self._process_batch()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=max(0.01, poll_interval_ms / 1000)
                )
            except TimeoutError:
                pass

    async def stop(self) -> None:
        self._running = False
        self._stop_event.set()

    async def _process_batch(self) -> int:
        projection_checkpoints = {name: await p.get_checkpoint() for name, p in self._projections.items()}
        min_checkpoint = min(projection_checkpoints.values(), default=0)
        batch: list[StoredEvent] = []
        async for event in self._store.load_all(
            from_global_position=min_checkpoint, batch_size=self._batch_size
        ):
            batch.append(event)
            if len(batch) >= self._batch_size:
                break
        if not batch:
            return 0

        for event in batch:
            subscribers = self._event_map.get(event.event_type, [])
            if not subscribers:
                continue
            for projection in subscribers:
                if event.global_position <= projection_checkpoints[projection.name]:
                    continue
                handled = False
                for attempt in range(1, self._max_retries + 1):
                    try:
                        await projection.apply_event_and_checkpoint(event)
                        projection_checkpoints[projection.name] = event.global_position
                        self._metrics.events_processed += 1
                        handled = True
                        break
                    except Exception as exc:
                        self._metrics.failures += 1
                        logger.exception(
                            "projection handler failure projection=%s event=%s gp=%s attempt=%s",
                            projection.name,
                            event.event_type,
                            event.global_position,
                            attempt,
                            exc_info=exc,
                        )
                if not handled:
                    logger.error(
                        "projection skipped bad event projection=%s event=%s gp=%s",
                        projection.name,
                        event.event_type,
                        event.global_position,
                    )
                    await projection.update_checkpoint(event.global_position)
                    projection_checkpoints[projection.name] = event.global_position
        self._metrics.batches_processed += 1
        return len(batch)

    async def get_lag(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for name, projection in self._projections.items():
            out[name] = await projection.get_lag()
        return out

    def get_metrics(self) -> dict[str, float | int]:
        return {
            "events_processed": self._metrics.events_processed,
            "batches_processed": self._metrics.batches_processed,
            "failures": self._metrics.failures,
            "processed_events_per_sec": round(self._metrics.processed_events_per_sec, 3),
        }

    async def get_health(
        self,
        *,
        slo_max_lag_events: int | None = None,
    ) -> dict[str, Any]:
        """Lag per projection, event-store head, and SLO bit (``slo_projection_catchup_ok``)."""
        max_allowed = (
            slo_max_lag_events
            if slo_max_lag_events is not None
            else int(os.environ.get("LEDGER_PROJECTION_MAX_LAG_EVENTS", "100000"))
        )
        lags = await self.get_lag()
        max_lag = max(lags.values(), default=0)
        head: int | None = None
        try:
            pool = self._store._require_pool()
        except (RuntimeError, AttributeError):
            pool = None
        if pool is not None:
            async with pool.acquire() as conn:
                head = int(
                    await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events")
                )
        return {
            "projection_lag_by_name": lags,
            "max_projection_lag": max_lag,
            "event_store_head_global_position": head,
            "slo_max_lag_events": max_allowed,
            "slo_projection_catchup_ok": max_lag <= max_allowed,
        }


