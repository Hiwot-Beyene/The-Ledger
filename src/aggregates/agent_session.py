from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from aggregates.replay import (
    advance_version_from_stored,
    event_type_from_stored,
    payload_from_stored,
)
from models.events import DomainError, StoredEvent

DECISION_TRACE_EVENT_TYPES = frozenset(
    {
        "CreditAnalysisCompleted",
        "FraudScreeningCompleted",
        "ComplianceCheckCompleted",
        "DecisionGenerated",
    }
)

CONTEXT_RULE = "gas_town_context_requirement"
MODEL_VERSION_RULE = "model_version_locking"
CAUSAL_CHAIN_RULE = "causal_chain_enforcement"

ERROR_MESSAGES = {
    "agent_context_required": "AgentContextLoaded must be the first event before decisions",
    "model_version_mismatch": (
        "Session model_version={session_model_version} does not match command={command_model_version}"
    ),
    "invalid_causal_chain": "Agent session did not process application {application_id}",
}


class EventStoreLike(Protocol):
    async def load_stream(self, stream_id: str) -> list[StoredEvent]: ...


@dataclass(slots=True)
class AgentSessionAggregate:
    agent_id: str
    session_id: str
    version: int = -1
    context_loaded: bool = False
    context_declared_first: bool = False
    model_version: str | None = None
    decision_application_ids: set[str] = field(default_factory=set)

    @staticmethod
    def parse_contributing_stream_id(
        stream_id: str,
        *,
        application_id: str,
        loan_application_state: str,
    ) -> tuple[str, str]:
        try:
            if not stream_id.startswith("agent-"):
                raise ValueError
            parts = stream_id.split("-", 2)
            if len(parts) != 3 or not parts[1] or not parts[2]:
                raise ValueError
            return parts[1], parts[2]
        except ValueError:
            raise DomainError(
                code="invalid_agent_stream_id",
                message=f"Invalid agent stream id: {stream_id}",
                aggregate_id=application_id,
                current_state=loan_application_state,
                rule=CAUSAL_CHAIN_RULE,
            ) from None

    @classmethod
    async def load(
        cls, store: EventStoreLike, agent_id: str, session_id: str
    ) -> "AgentSessionAggregate":
        stream_id = f"agent-{agent_id}-{session_id}"
        events = await store.load_stream(stream_id)
        agg = cls(agent_id=agent_id, session_id=session_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent | dict[str, Any]) -> None:
        event_type = event_type_from_stored(event)
        handler = getattr(self, f"_on_{event_type}", None)
        if handler is not None:
            handler(event)
        self.version = advance_version_from_stored(event, self.version)

    def _track_application_from_payload(self, payload: dict[str, Any]) -> None:
        app_id = payload.get("application_id")
        if app_id:
            self.decision_application_ids.add(str(app_id))

    def _track_application_if_present(self, event: StoredEvent | dict[str, Any]) -> None:
        self._track_application_from_payload(payload_from_stored(event))

    def _raise_domain_error(self, code: str, current_state: str, rule: str, **message_values: Any) -> None:
        message = ERROR_MESSAGES[code].format(**message_values)
        raise DomainError(
            code=code,
            message=message,
            aggregate_id=f"agent-{self.agent_id}-{self.session_id}",
            current_state=current_state,
            rule=rule,
        )

    def _on_AgentContextLoaded(self, event: StoredEvent | dict[str, Any]) -> None:
        payload = payload_from_stored(event)
        if self.version == -1:
            self.context_declared_first = True
        self.context_loaded = True
        self.model_version = str(payload.get("model_version", ""))
        self._track_application_from_payload(payload)

    def _on_AgentSessionStarted(self, event: StoredEvent | dict[str, Any]) -> None:
        payload = payload_from_stored(event)
        if self.version == -1:
            self.context_declared_first = False
        if payload.get("model_version") and self.model_version is None:
            self.model_version = str(payload["model_version"])

    def _on_CreditAnalysisCompleted(self, event: StoredEvent | dict[str, Any]) -> None:
        self._track_application_if_present(event)

    def _on_FraudScreeningCompleted(self, event: StoredEvent | dict[str, Any]) -> None:
        self._track_application_if_present(event)

    def _on_ComplianceCheckCompleted(self, event: StoredEvent | dict[str, Any]) -> None:
        self._track_application_if_present(event)

    def _on_DecisionGenerated(self, event: StoredEvent | dict[str, Any]) -> None:
        self._track_application_if_present(event)

    def _on_AgentOutputWritten(self, event: StoredEvent | dict[str, Any]) -> None:
        payload = payload_from_stored(event)
        app_id = payload.get("application_id")
        if not app_id:
            return
        for entry in payload.get("events_written", []):
            if str(entry.get("event_type", "")) in DECISION_TRACE_EVENT_TYPES:
                self.decision_application_ids.add(str(app_id))
                break

    def assert_context_loaded(self) -> None:
        if not self.context_loaded or not self.context_declared_first:
            self._raise_domain_error(
                "agent_context_required",
                current_state="NO_CONTEXT",
                rule=CONTEXT_RULE,
            )

    def assert_model_version_current(self, model_version: str) -> None:
        if self.model_version is not None and self.model_version != model_version:
            self._raise_domain_error(
                "model_version_mismatch",
                current_state=self.model_version,
                rule=MODEL_VERSION_RULE,
                session_model_version=self.model_version,
                command_model_version=model_version,
            )

    def assert_session_processed_application(self, application_id: str) -> None:
        if application_id not in self.decision_application_ids:
            self._raise_domain_error(
                "invalid_causal_chain",
                current_state="NO_APPLICATION_CONTEXT",
                rule=CAUSAL_CHAIN_RULE,
                application_id=application_id,
            )
