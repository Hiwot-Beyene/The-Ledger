from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from ledger.domain.aggregates.replay import (
    advance_version_from_stored,
    event_type_from_stored,
    payload_from_stored,
    rules_evaluate_ids,
)
from ledger.schema.events import StoredEvent


class EventStoreLike(Protocol):
    async def load_stream(self, stream_id: str) -> list[StoredEvent]: ...


@dataclass(slots=True)
class ComplianceRecordAggregate:
    application_id: str
    version: int = -1
    required_checks: set[str] = field(default_factory=set)
    passed_checks: set[str] = field(default_factory=set)

    @classmethod
    async def load(
        cls, store: EventStoreLike, application_id: str
    ) -> "ComplianceRecordAggregate":
        events = await store.load_stream(f"compliance-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent | dict[str, Any]) -> None:
        event_type = event_type_from_stored(event)
        payload = payload_from_stored(event)
        if event_type in {"ComplianceCheckInitiated", "ComplianceCheckRequested"}:
            self.required_checks = rules_evaluate_ids(payload)
        if event_type == "ComplianceRulePassed":
            rule_id = payload.get("rule_id")
            if rule_id:
                self.passed_checks.add(str(rule_id))
        self.version = advance_version_from_stored(event, self.version)
