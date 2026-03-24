from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Callable, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from event_store import EventStore, InMemoryEventStore
from models.events import BaseEvent, StoredEvent

StoreT = EventStore | InMemoryEventStore

_APP_STREAM_PREFIXES = ("loan-", "credit-", "compliance-", "fraud-", "docpkg-")


class ReplayProjection(Protocol):
    name: str

    def apply(self, event: StoredEvent) -> None: ...

    def snapshot(self) -> dict[str, Any]: ...


@dataclass(slots=True)
class DivergenceEvent:
    reason: str
    event_id: str
    event_type: str
    stream_id: str


@dataclass(slots=True)
class WhatIfResult:
    branch_event_id: str
    real_outcome: dict[str, Any]
    counterfactual_outcome: dict[str, Any]
    divergence_events: list[DivergenceEvent]


class TimeTravelError(ValueError):
    pass


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Unsupported json type: {type(value).__name__}")


def _is_application_event(event: StoredEvent, application_id: str) -> bool:
    if str(event.payload.get("application_id", "")).strip() == application_id:
        return True
    return any(event.stream_id == f"{prefix}{application_id}" for prefix in _APP_STREAM_PREFIXES)


def _event_time(event: StoredEvent) -> datetime:
    payload = event.payload

    def _as_utc(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    for key in (
        "submitted_at",
        "uploaded_at",
        "requested_at",
        "generated_at",
        "completed_at",
        "reviewed_at",
        "approved_at",
        "declined_at",
        "initiated_at",
        "evaluated_at",
        "detected_at",
        "written_at",
        "failed_at",
        "loaded_at",
        "started_at",
        "executed_at",
        "called_at",
    ):
        raw = payload.get(key)
        if isinstance(raw, str):
            return _as_utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))
        if isinstance(raw, datetime):
            return _as_utc(raw)
    return _as_utc(event.recorded_at)


class CausalDependencyAnalyzer:
    def transitive_dependents(
        self,
        *,
        all_events: list[StoredEvent],
        root_event_ids: set[str],
    ) -> set[str]:
        blocked = set(root_event_ids)
        changed = True
        while changed:
            changed = False
            for event in all_events:
                event_id = str(event.event_id)
                if event_id in blocked:
                    continue
                causation_id = str(event.metadata.get("causation_id", "")).strip()
                if causation_id and causation_id in blocked:
                    blocked.add(event_id)
                    changed = True
        return blocked


class ReplayEngine:
    def replay(
        self,
        events: list[StoredEvent],
        projection_factories: list[Callable[[], ReplayProjection]],
    ) -> dict[str, dict[str, Any]]:
        projections = [factory() for factory in projection_factories]
        for event in events:
            for projection in projections:
                projection.apply(event)
        return {projection.name: projection.snapshot() for projection in projections}


class ResultComparator:
    def compare(
        self,
        real_outcome: dict[str, Any],
        counterfactual_outcome: dict[str, Any],
    ) -> dict[str, Any]:
        divergence: dict[str, Any] = {}
        keys = set(real_outcome) | set(counterfactual_outcome)
        for key in sorted(keys):
            if real_outcome.get(key) != counterfactual_outcome.get(key):
                divergence[key] = {
                    "real": real_outcome.get(key),
                    "counterfactual": counterfactual_outcome.get(key),
                }
        return divergence


class ApplicationSummaryReplayProjection:
    name = "application_summary"

    def __init__(self) -> None:
        self._state: dict[str, Any] = {
            "application_id": None,
            "state": None,
            "risk_tier": None,
            "fraud_score": None,
            "compliance_status": None,
            "decision": None,
            "approved_amount_usd": None,
            "final_decision_at": None,
            "last_event_type": None,
        }

    def apply(self, event: StoredEvent) -> None:
        payload = event.payload
        et = event.event_type
        if payload.get("application_id"):
            self._state["application_id"] = payload.get("application_id")
        if et == "ApplicationSubmitted":
            self._state["state"] = "SUBMITTED"
        elif et == "CreditAnalysisCompleted":
            self._state["state"] = "CREDIT_ANALYSIS_COMPLETE"
            risk = payload.get("risk_tier")
            limit = payload.get("recommended_limit_usd")
            decision = payload.get("decision")
            if isinstance(decision, dict):
                risk = risk or decision.get("risk_tier")
                limit = limit or decision.get("recommended_limit_usd")
            self._state["risk_tier"] = risk
            self._state["approved_amount_usd"] = limit
        elif et == "FraudScreeningCompleted":
            self._state["state"] = "FRAUD_SCREENING_COMPLETE"
            self._state["fraud_score"] = payload.get("fraud_score")
        elif et == "ComplianceCheckCompleted":
            self._state["state"] = "COMPLIANCE_CHECK_COMPLETE"
            self._state["compliance_status"] = payload.get("overall_verdict")
        elif et == "DecisionGenerated":
            recommendation = str(payload.get("recommendation", "")).upper()
            self._state["decision"] = recommendation
            if recommendation == "APPROVE":
                self._state["state"] = "APPROVED"
            elif recommendation == "DECLINE":
                self._state["state"] = "DECLINED"
            else:
                self._state["state"] = "PENDING_HUMAN_REVIEW"
        elif et == "ApplicationApproved":
            self._state["state"] = "APPROVED"
            self._state["decision"] = "APPROVE"
            self._state["approved_amount_usd"] = payload.get("approved_amount_usd")
            self._state["final_decision_at"] = payload.get("approved_at")
        elif et == "ApplicationDeclined":
            self._state["state"] = "DECLINED"
            self._state["decision"] = "DECLINE"
            self._state["final_decision_at"] = payload.get("declined_at")
        self._state["last_event_type"] = et

    def snapshot(self) -> dict[str, Any]:
        return dict(self._state)


class AgentPerformanceReplayProjection:
    name = "agent_performance_ledger"

    def __init__(self) -> None:
        self._stats: dict[str, dict[str, Any]] = {}

    def apply(self, event: StoredEvent) -> None:
        if event.event_type != "AgentSessionCompleted":
            return
        payload = event.payload
        agent_type = str(payload.get("agent_type") or "unknown")
        model_version = str(payload.get("model_version") or "unknown")
        key = f"{agent_type}:{model_version}"
        row = self._stats.setdefault(
            key,
            {
                "agent_type": agent_type,
                "model_version": model_version,
                "sessions": 0,
                "llm_calls": 0,
                "tokens": 0,
                "cost_usd": 0.0,
            },
        )
        row["sessions"] += 1
        row["llm_calls"] += int(payload.get("total_llm_calls") or 0)
        row["tokens"] += int(payload.get("total_tokens_used") or 0)
        row["cost_usd"] = round(float(row["cost_usd"]) + float(payload.get("total_cost_usd") or 0.0), 6)

    def snapshot(self) -> dict[str, Any]:
        return {"rows": sorted(self._stats.values(), key=lambda r: (r["agent_type"], r["model_version"]))}


class ComplianceAuditReplayProjection:
    name = "compliance_audit_view"

    def __init__(self) -> None:
        self._state: dict[str, Any] = {
            "application_id": None,
            "overall_verdict": None,
            "has_hard_block": None,
            "rules_evaluated": 0,
            "rules_passed": 0,
            "rules_failed": 0,
            "rules_noted": 0,
            "updated_at": None,
        }

    def apply(self, event: StoredEvent) -> None:
        if event.event_type != "ComplianceCheckCompleted":
            return
        payload = event.payload
        self._state["application_id"] = payload.get("application_id")
        verdict = payload.get("overall_verdict")
        if isinstance(verdict, dict) and "value" in verdict:
            verdict = verdict.get("value")
        elif verdict is not None and hasattr(verdict, "value"):
            verdict = getattr(verdict, "value")
        self._state["overall_verdict"] = verdict
        self._state["has_hard_block"] = bool(payload.get("has_hard_block", False))
        self._state["rules_evaluated"] = int(payload.get("rules_evaluated") or 0)
        self._state["rules_passed"] = int(payload.get("rules_passed") or 0)
        self._state["rules_failed"] = int(payload.get("rules_failed") or 0)
        self._state["rules_noted"] = int(payload.get("rules_noted") or 0)
        self._state["updated_at"] = event.recorded_at.astimezone(UTC).isoformat()

    def snapshot(self) -> dict[str, Any]:
        return dict(self._state)


class ProjectionRunner:
    def __init__(self, replay_engine: ReplayEngine | None = None) -> None:
        self._engine = replay_engine or ReplayEngine()

    def run(
        self,
        *,
        events: list[StoredEvent],
        projection_factories: list[Callable[[], ReplayProjection]],
    ) -> dict[str, dict[str, Any]]:
        return self._engine.replay(events, projection_factories)


async def _load_application_events(
    store: StoreT,
    application_id: str,
    *,
    from_global_position: int = 0,
) -> list[StoredEvent]:
    out: list[StoredEvent] = []
    async for event in store.load_all(from_global_position=from_global_position, batch_size=500):
        if _is_application_event(event, application_id):
            out.append(event)
    out.sort(key=lambda e: e.global_position)
    return out


async def compliance_state_as_of_from_events(
    store: StoreT,
    application_id: str,
    as_of: datetime,
) -> dict[str, Any]:
    """Replay compliance projection on events committed on/before ``as_of`` (``recorded_at``, audit clock)."""
    if as_of.tzinfo is None:
        raise TimeTravelError("as_of must be timezone-aware")
    all_app_events = await _load_application_events(store, application_id)
    boundary = as_of.astimezone(UTC)

    def _visible_as_of(e: StoredEvent) -> bool:
        t_rec = e.recorded_at.astimezone(UTC)
        try:
            t_bus = _event_time(e)
        except Exception:
            t_bus = t_rec
        return t_rec <= boundary or t_bus <= boundary

    as_of_events = [e for e in all_app_events if _visible_as_of(e)]
    runner = ProjectionRunner()
    states = runner.run(events=as_of_events, projection_factories=[ComplianceAuditReplayProjection])
    return states[ComplianceAuditReplayProjection.name]


def _build_counterfactual_events(
    *,
    counterfactual_events: list[BaseEvent | dict[str, Any]],
    anchor_event: StoredEvent,
) -> list[StoredEvent]:
    synthetic: list[StoredEvent] = []
    for idx, event in enumerate(counterfactual_events, start=1):
        if isinstance(event, BaseEvent):
            event_type = event.event_type
            event_version = int(event.event_version)
            payload = event.to_payload()
        else:
            if "event_type" not in event:
                raise TimeTravelError("counterfactual event dict requires event_type")
            event_type = str(event["event_type"])
            event_version = int(event.get("event_version", 1))
            payload = dict(event.get("payload", {}))
        event_id = uuid5(
            NAMESPACE_URL,
            f"what-if:{anchor_event.stream_id}:{anchor_event.stream_position}:{event_type}:{idx}",
        )
        synthetic.append(
            StoredEvent(
                event_id=event_id,
                stream_id=anchor_event.stream_id,
                stream_position=anchor_event.stream_position,
                global_position=anchor_event.global_position,
                event_type=event_type,
                event_version=event_version,
                payload=payload,
                metadata={
                    "what_if": True,
                    "synthetic": True,
                    "replaces_event_id": str(anchor_event.event_id),
                    "causation_id": str(anchor_event.metadata.get("causation_id", "")) or None,
                },
                recorded_at=anchor_event.recorded_at,
            )
        )
    return synthetic


async def run_what_if(
    store: StoreT,
    application_id: str,
    branch_at_event_type: str,
    counterfactual_events: list[BaseEvent | dict[str, Any]],
    projections: list[Callable[[], ReplayProjection]] | None = None,
) -> WhatIfResult:
    if not counterfactual_events:
        raise TimeTravelError("counterfactual_events must not be empty")
    projection_factories = projections or [
        ApplicationSummaryReplayProjection,
        AgentPerformanceReplayProjection,
        ComplianceAuditReplayProjection,
    ]
    app_events = await _load_application_events(store, application_id)
    if not app_events:
        raise TimeTravelError(f"No events found for application_id={application_id}")
    branch_event = next((e for e in app_events if e.event_type == branch_at_event_type), None)
    if branch_event is None:
        raise TimeTravelError(f"branch_at_event_type={branch_at_event_type!r} not present in history")
    branch_idx = app_events.index(branch_event)

    analyzer = CausalDependencyAnalyzer()
    blocked = analyzer.transitive_dependents(
        all_events=app_events[branch_idx + 1 :],
        root_event_ids={str(branch_event.event_id)},
    )

    pre_branch = app_events[:branch_idx]
    post_branch_independent = [e for e in app_events[branch_idx + 1 :] if str(e.event_id) not in blocked]
    synthetic = _build_counterfactual_events(
        counterfactual_events=counterfactual_events,
        anchor_event=branch_event,
    )
    counterfactual_timeline = [*pre_branch, *synthetic, *post_branch_independent]
    real_timeline = list(app_events)

    runner = ProjectionRunner()
    real_outcome = runner.run(events=real_timeline, projection_factories=projection_factories)
    counterfactual_outcome = runner.run(
        events=counterfactual_timeline,
        projection_factories=projection_factories,
    )
    divergence = ResultComparator().compare(real_outcome, counterfactual_outcome)

    divergence_events: list[DivergenceEvent] = [
        DivergenceEvent(
            reason="replaced_at_branch",
            event_id=str(branch_event.event_id),
            event_type=branch_event.event_type,
            stream_id=branch_event.stream_id,
        )
    ]
    for event in app_events[branch_idx + 1 :]:
        if str(event.event_id) in blocked:
            divergence_events.append(
                DivergenceEvent(
                    reason="causally_dependent_excluded",
                    event_id=str(event.event_id),
                    event_type=event.event_type,
                    stream_id=event.stream_id,
                )
            )
    if divergence:
        divergence_events.append(
            DivergenceEvent(
                reason="outcome_diverged",
                event_id=str(branch_event.event_id),
                event_type=branch_event.event_type,
                stream_id=branch_event.stream_id,
            )
        )

    return WhatIfResult(
        branch_event_id=str(branch_event.event_id),
        real_outcome=real_outcome,
        counterfactual_outcome=counterfactual_outcome,
        divergence_events=divergence_events,
    )


