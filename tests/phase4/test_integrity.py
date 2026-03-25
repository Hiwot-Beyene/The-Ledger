from __future__ import annotations

from datetime import UTC, datetime

import pytest

from event_store import InMemoryEventStore
from integrity import run_integrity_check


@pytest.mark.asyncio
async def test_integrity_chain_detects_tampering() -> None:
    store = InMemoryEventStore()
    stream_id = "loan-app-integrity-1"
    await store.append(
        stream_id,
        [
            {"event_type": "ApplicationSubmitted", "event_version": 1, "payload": {"application_id": "app-1"}},
            {"event_type": "DecisionRequested", "event_version": 1, "payload": {"application_id": "app-1"}},
        ],
        expected_version=-1,
    )
    first = await run_integrity_check(store, "loan", "app-integrity-1")
    assert first.chain_valid is True
    assert first.tamper_detected is False

    # Simulated post-hoc mutation in memory to exercise detection behavior.
    store._streams[stream_id][0].payload["application_id"] = "tampered"  # type: ignore[attr-defined]
    second = await run_integrity_check(store, "loan", "app-integrity-1")
    assert second.chain_valid is False
    assert second.tamper_detected is True

    audit_events = await store.load_stream("audit-loan-app-integrity-1")
    assert audit_events[-1].event_type == "AuditIntegrityCheckRun"
    assert audit_events[-1].payload["chain_valid"] is False
