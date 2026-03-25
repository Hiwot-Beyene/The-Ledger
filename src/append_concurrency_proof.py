"""Loan-stream append race using the same ``EventStore.append`` CAS path as production.

Two coroutines call ``append`` with the same ``expected_version``; one wins, the other
receives ``OptimisticConcurrencyError`` and retries after reloading the version — the same
pattern as ``BaseApexAgent._append_stream`` retries.

Events appended are real domain types already used on ``loan-{{id}}`` (see credit pipeline:
``FraudScreeningRequested``), not placeholder demo records.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from event_store import EventStore
from models.events import FraudScreeningRequested, OptimisticConcurrencyError, StoredEvent

PROOF_CORRELATION_PREFIX = "append-concurrency-proof"
WRITERS = ("agent-a", "agent-b")
PROOF_EVENT_TYPE = "FraudScreeningRequested"


async def reconcile_stream_head_version(store: EventStore, stream_id: str) -> tuple[int, bool]:
    """Return ``(current_version, repaired)``; ``repaired`` if ``event_streams`` was aligned from ``events`` tail."""
    v = await store.stream_version(stream_id)
    if v >= 0:
        return v, False
    pool = store._require_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COALESCE(MAX(stream_position), 0)::bigint AS mx FROM events WHERE stream_id = $1",
            stream_id,
        )
        mx = int(row["mx"]) if row else 0
    if mx <= 0:
        return -1, False
    aggregate_type = stream_id.split("-", 1)[0]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO event_streams (stream_id, aggregate_type, current_version, metadata)
                VALUES ($1, $2, $3, '{}'::jsonb)
                ON CONFLICT (stream_id) DO UPDATE SET
                  current_version = GREATEST(event_streams.current_version, EXCLUDED.current_version)
                """,
                stream_id,
                aggregate_type,
                mx,
            )
    v2 = await store.stream_version(stream_id)
    return v2, True


def _proof_fraud_screening_requested(application_id: str) -> dict[str, Any]:
    return FraudScreeningRequested(
        application_id=application_id,
        requested_at=datetime.now(UTC),
        triggered_by_event_id=str(uuid4()),
    ).to_store_dict()


def loan_append_concurrency_proof_plan_payload(
    application_id: str, stream_id: str, v: int, repaired: bool
) -> dict[str, Any]:
    aid = application_id.strip()
    return {
        "ok": True,
        "application_id": aid,
        "stream_id": stream_id,
        "stream_kind": "loan_aggregate",
        "plan_snapshot_at_utc": datetime.now(UTC).isoformat(),
        "append_path": "event_store.EventStore.append",
        "proof_event_type": PROOF_EVENT_TYPE,
        "postgres_observation": {
            "event_streams.current_version": v,
            "used_as_expected_version_for_concurrent_append": v,
            "reconciled_event_streams_from_max_stream_position": repaired,
        },
        "mechanism": {
            "cas": (
                "UPDATE event_streams SET current_version = current_version + $2 "
                "WHERE stream_id = $1 AND current_version = $3 AND archived_at IS NULL RETURNING current_version"
            ),
            "why_conflict": (
                "Both writers submit the same expected_version; the first commit increments current_version; "
                "the second CAS fails → OptimisticConcurrencyError."
            ),
            "retry_pattern": "Reload stream_version (winner's commit) and append again — same as agent OCC retries.",
        },
        "race_steps": {
            "expected_version": v,
            "writers": [
                {
                    "agent": agent,
                    "append": {
                        "event_type": PROOF_EVENT_TYPE,
                        "expected_version": v,
                        "causation_id": f"{PROOF_CORRELATION_PREFIX}:{agent}",
                        "correlation_id": f"{PROOF_CORRELATION_PREFIX}:{aid}",
                        "payload_shape": (
                            "application_id, requested_at, triggered_by_event_id (unique UUID per append)"
                        ),
                    },
                }
                for agent in WRITERS
            ],
        },
        "retry_step": {
            "expected_version_source": "await stream_version() after the race",
            "append": {
                "event_type": PROOF_EVENT_TYPE,
                "correlation_id": "retry-after-occ-collision",
                "note": "Another FraudScreeningRequested with fresh triggered_by_event_id",
            },
        },
        "disclaimer": (
            "Appends real FraudScreeningRequested events to the loan aggregate stream (same type the credit "
            "agent emits when advancing the workflow). Visible on the permanent audit trail — use a sandbox app id if needed."
        ),
    }


def _observed_event_row(e: StoredEvent) -> dict[str, Any]:
    md = e.metadata if isinstance(e.metadata, dict) else {}
    return {
        "event_id": str(e.event_id),
        "stream_position": e.stream_position,
        "global_position": e.global_position,
        "event_type": e.event_type,
        "recorded_at": e.recorded_at.isoformat(),
        "causation_id": md.get("causation_id"),
        "correlation_id": md.get("correlation_id"),
    }


async def run_loan_append_concurrency_proof(store: EventStore, application_id: str) -> dict[str, Any]:
    t0 = time.perf_counter()
    started_at = datetime.now(UTC)
    aid = application_id.strip()
    stream_id = f"loan-{aid}"
    v, repaired = await reconcile_stream_head_version(store, stream_id)
    if v < 0:
        raise ValueError(
            f"No loan stream in database for application_id={aid!r} (no events and no event_streams row for {stream_id})."
        )

    barrier = asyncio.Barrier(2)
    results: list[dict[str, Any]] = []

    async def racer(agent: str) -> None:
        await barrier.wait()
        try:
            nv = await store.append(
                stream_id,
                [_proof_fraud_screening_requested(aid)],
                expected_version=v,
                causation_id=f"{PROOF_CORRELATION_PREFIX}:{agent}",
                correlation_id=f"{PROOF_CORRELATION_PREFIX}:{aid}",
            )
            results.append({"agent": agent, "outcome": "appended", "new_version": nv})
        except OptimisticConcurrencyError as e:
            results.append(
                {
                    "agent": agent,
                    "outcome": "OptimisticConcurrencyError",
                    "diagnostic": e.to_diagnostic_dict(),
                }
            )

    await asyncio.gather(racer(WRITERS[0]), racer(WRITERS[1]))
    loser = next((r for r in results if r.get("outcome") == "OptimisticConcurrencyError"), None)
    retry: dict[str, Any] | None = None
    if loser:
        agent = str(loser["agent"])
        try:
            ver = await store.stream_version(stream_id)
            nv = await store.append(
                stream_id,
                [_proof_fraud_screening_requested(aid)],
                expected_version=ver,
                correlation_id="retry-after-occ-collision",
                causation_id=f"{PROOF_CORRELATION_PREFIX}:retry:{agent}",
            )
            retry = {"agent": agent, "outcome": "retry_appended", "new_version": nv}
        except Exception as exc:  # pragma: no cover
            retry = {"agent": agent, "outcome": "retry_failed", "error": str(exc)}
    final_ver = await store.stream_version(stream_id)
    stream = await store.load_stream(stream_id)
    completed_at = datetime.now(UTC)
    duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    tail = stream[-15:]
    return {
        "ok": True,
        "application_id": aid,
        "stream_id": stream_id,
        "append_path": "event_store.EventStore.append",
        "proof_event_type": PROOF_EVENT_TYPE,
        "proof_run": {
            "started_at_utc": started_at.isoformat(),
            "completed_at_utc": completed_at.isoformat(),
            "duration_ms": duration_ms,
            "observation_source": "postgres_events_and_event_streams_after_append",
        },
        "event_streams_reconciled_from_events_table": repaired,
        "pre_race_stream_version_used_as_expected_version": v,
        "concurrent_results": results,
        "retry": retry,
        "final_stream_version": final_ver,
        "event_types_in_order_tail": [e.event_type for e in tail],
        "stream_tail_observed": [_observed_event_row(e) for e in tail],
    }
