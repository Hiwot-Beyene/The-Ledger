from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from models.events import StoredEvent


class EventStoreLike(Protocol):
    async def load_stream(self, stream_id: str, from_position: int = 0) -> list[StoredEvent]: ...
    async def append(
        self, stream_id: str, events: list[dict[str, Any]], expected_version: int, **kwargs: Any
    ) -> int: ...
    async def stream_version(self, stream_id: str) -> int: ...


@dataclass(slots=True)
class IntegrityCheckResult:
    events_verified: int
    chain_valid: bool
    tamper_detected: bool
    integrity_hash: str
    previous_hash: str | None


def _canonical_event_hash(event: StoredEvent) -> str:
    material = {
        "event_type": event.event_type,
        "event_version": int(event.event_version),
        "recorded_at": event.recorded_at.astimezone(UTC).isoformat(),
        "payload": event.payload,
    }
    blob = json.dumps(material, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def rolling_hash(previous_hash: str, events: list[StoredEvent]) -> str:
    h = previous_hash
    for event in events:
        h = hashlib.sha256((h + _canonical_event_hash(event)).encode("utf-8")).hexdigest()
    return h


def verify_audit_chain(
    entity_events: list[StoredEvent],
    audit_checks: list[StoredEvent],
) -> tuple[bool, bool, str | None, int]:
    previous_hash: str | None = None
    chain_valid = True
    tamper_detected = False
    prev_verified_count = 0

    for audit_event in audit_checks:
        payload = dict(audit_event.payload)
        if payload.get("previous_hash") != previous_hash:
            chain_valid = False
            tamper_detected = True
            break
        count = int(payload.get("events_verified_count", 0))
        if count < prev_verified_count or count > len(entity_events):
            chain_valid = False
            tamper_detected = True
            break
        expected_hash = rolling_hash(previous_hash or "", entity_events[prev_verified_count:count])
        if payload.get("integrity_hash") != expected_hash:
            chain_valid = False
            tamper_detected = True
            break
        previous_hash = str(payload.get("integrity_hash"))
        prev_verified_count = count

    return chain_valid, tamper_detected, previous_hash, prev_verified_count


async def run_integrity_check(
    store: EventStoreLike,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    primary_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"

    entity_events = await store.load_stream(primary_stream)
    audit_events = [e for e in await store.load_stream(audit_stream) if e.event_type == "AuditIntegrityCheckRun"]

    chain_valid, tamper_detected, previous_hash, prev_verified_count = verify_audit_chain(
        entity_events,
        audit_events,
    )

    base_hash = previous_hash or ""
    current_hash = rolling_hash(base_hash, entity_events[prev_verified_count:])
    final_verified_count = len(entity_events)

    expected_version = await store.stream_version(audit_stream)
    check_event = {
        "event_type": "AuditIntegrityCheckRun",
        "event_version": 1,
        "payload": {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "check_timestamp": datetime.now(UTC).isoformat(),
            "events_verified_count": final_verified_count,
            "integrity_hash": current_hash,
            "previous_hash": previous_hash,
            "chain_valid": chain_valid,
            "tamper_detected": tamper_detected,
        },
    }
    await store.append(audit_stream, [check_event], expected_version=expected_version)
    return IntegrityCheckResult(
        events_verified=final_verified_count,
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
        integrity_hash=current_hash,
        previous_hash=previous_hash,
    )
