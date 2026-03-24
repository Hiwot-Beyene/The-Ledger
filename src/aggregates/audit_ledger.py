from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from aggregates.replay import event_type_from_stored, payload_from_stored
from models.events import DomainError, StoredEvent


@dataclass(slots=True)
class AuditLedgerAggregate:
    entity_type: str
    entity_id: str
    last_hash: str | None = None
    chain_valid: bool = True
    checked_events: int = 0
    seen_stream_positions: dict[str, int] = field(default_factory=dict)

    def apply(self, event: StoredEvent | dict[str, Any]) -> None:
        event_type = event_type_from_stored(event)
        if event_type != "AuditIntegrityCheckRun":
            return
        payload = payload_from_stored(event)
        stream_key = f"{payload.get('entity_type')}:{payload.get('entity_id')}"
        pos = int(payload.get("events_verified_count", 0))
        prev = self.seen_stream_positions.get(stream_key, -1)
        if pos < prev:
            raise DomainError(
                code="audit_causal_order_violation",
                message="Audit checks must be append-only and causally ordered.",
                aggregate_id=str(self.entity_id),
                current_state="AUDIT_LEDGER",
                rule="cross_stream_causal_ordering",
            )
        self.seen_stream_positions[stream_key] = pos
        self.last_hash = str(payload.get("integrity_hash") or "")
        self.chain_valid = bool(payload.get("chain_valid", True))
        self.checked_events = pos

    def assert_append_only(self) -> None:
        if not self.chain_valid:
            raise DomainError(
                code="audit_chain_invalid",
                message="Audit chain has invalid link(s).",
                aggregate_id=str(self.entity_id),
                current_state="AUDIT_LEDGER",
                rule="append_only_enforcement",
            )
