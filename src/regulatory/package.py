"""Regulatory examination JSON package and independent hash verification."""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Callable
from uuid import UUID

from event_store import EventStore, InMemoryEventStore
from integrity.audit_chain import verify_audit_chain, rolling_hash
from models.events import StoredEvent

from what_if.projector import (
    ApplicationSummaryReplayProjection,
    AgentPerformanceReplayProjection,
    ComplianceAuditReplayProjection,
    ProjectionRunner,
    ReplayProjection,
    TimeTravelError,
    _event_time,
    _json_default,
    _load_application_events,
)

StoreT = EventStore | InMemoryEventStore

def _event_to_raw(event: StoredEvent) -> dict[str, Any]:
    return {
        "event_id": str(event.event_id),
        "stream_id": event.stream_id,
        "stream_position": event.stream_position,
        "global_position": event.global_position,
        "event_type": event.event_type,
        "event_version": event.event_version,
        "payload": event.payload,
        "metadata": event.metadata,
        "recorded_at": event.recorded_at.astimezone(UTC).isoformat(),
    }


def _session_anchor_event_ids(events: list[StoredEvent]) -> dict[str, str]:
    by_session: dict[str, list[StoredEvent]] = {}
    for e in events:
        sid = e.payload.get("session_id")
        if sid is None:
            continue
        key = str(sid).strip()
        if key:
            by_session.setdefault(key, []).append(e)
    out: dict[str, str] = {}
    for sid, evs in by_session.items():
        first = min(evs, key=lambda x: (x.global_position, x.stream_id, x.stream_position))
        out[sid] = str(first.event_id)
    return out


def _resolve_cause_ref(
    ref: str,
    id_set: set[str],
    session_anchors: dict[str, str],
) -> tuple[str | None, str]:
    if ref in id_set:
        return ref, "event_id"
    if ref in session_anchors:
        return session_anchors[ref], "session_anchor"
    return None, ""


_PAYLOAD_CAUSAL_KEYS: tuple[str, ...] = (
    "triggered_by_event_id",
    "caused_by_event_id",
    "parent_event_id",
)


def _causal_links(events: list[StoredEvent]) -> list[dict[str, Any]]:
    id_set = {str(e.event_id) for e in events}
    session_anchors = _session_anchor_event_ids(events)
    links: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def add_link(
        cause_eid: str,
        effect: StoredEvent,
        *,
        link_kind: str,
        payload_field: str | None = None,
    ) -> None:
        key = (cause_eid, str(effect.event_id), link_kind)
        if key in seen:
            return
        seen.add(key)
        cor = effect.metadata.get("correlation_id")
        row: dict[str, Any] = {
            "cause_event_id": cause_eid,
            "effect_event_id": str(effect.event_id),
            "effect_event_type": effect.event_type,
            "effect_stream_id": effect.stream_id,
            "correlation_id": cor if cor is None else str(cor),
            "link_kind": link_kind,
        }
        if payload_field:
            row["payload_field"] = payload_field
        links.append(row)

    for e in events:
        raw_c = e.metadata.get("causation_id")
        cid = str(raw_c).strip() if raw_c is not None else ""
        if cid:
            cause_eid, kind = _resolve_cause_ref(cid, id_set, session_anchors)
            if cause_eid:
                add_link(cause_eid, e, link_kind=kind if kind else "event_id")
        pl = e.payload
        for pk in _PAYLOAD_CAUSAL_KEYS:
            ref = pl.get(pk)
            if ref is None or ref == "":
                continue
            rs = str(ref).strip()
            cause_eid, kind = _resolve_cause_ref(rs, id_set, session_anchors)
            if cause_eid:
                add_link(cause_eid, e, link_kind=kind or "payload_ref", payload_field=pk)

    return links


def _integrity_hash(events: list[StoredEvent], previous_hash: str = "") -> str:
    return rolling_hash(previous_hash, events)


def _narrative_sentence(event: StoredEvent) -> str | None:
    payload = event.payload
    et = event.event_type
    app_id = payload.get("application_id", "unknown")
    if et == "ApplicationSubmitted":
        return f"Application {app_id} was submitted for USD {payload.get('requested_amount_usd')}."
    if et == "CreditAnalysisCompleted":
        return (
            f"Credit analysis completed with risk tier {payload.get('risk_tier')} "
            f"at confidence {payload.get('confidence_score')}."
        )
    if et == "FraudScreeningCompleted":
        return f"Fraud screening completed with score {payload.get('fraud_score')}."
    if et == "ComplianceCheckCompleted":
        return f"Compliance check completed with verdict {payload.get('overall_verdict')}."
    if et == "DecisionGenerated":
        return f"Decision orchestrator recommended {payload.get('recommendation')}."
    if et == "HumanReviewCompleted":
        return f"Human review finalized decision as {payload.get('final_decision')}."
    if et == "ApplicationApproved":
        return f"Application was approved for USD {payload.get('approved_amount_usd')}."
    if et == "ApplicationDeclined":
        return "Application was declined."
    return None


def _extract_ai_trace(events: list[StoredEvent]) -> list[dict[str, Any]]:
    traces: list[dict[str, Any]] = []
    for event in events:
        payload = event.payload
        model_version = payload.get("model_version")
        model_versions = payload.get("model_versions")
        confidence = payload.get("confidence_score")
        input_data_hash = payload.get("input_data_hash")
        if model_version or model_versions or confidence is not None or input_data_hash:
            traces.append(
                {
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "model_version": model_version,
                    "model_versions": model_versions,
                    "confidence_score": confidence,
                    "input_data_hash": input_data_hash,
                    "recorded_at": event.recorded_at.astimezone(UTC).isoformat(),
                }
            )
    return traces


async def generate_regulatory_package(
    store: StoreT,
    application_id: str,
    examination_date: datetime,
    projections: list[Callable[[], ReplayProjection]] | None = None,
    output_path: str | None = None,
) -> dict[str, Any]:
    if examination_date.tzinfo is None:
        raise TimeTravelError("examination_date must be timezone-aware")
    projection_factories = projections or [
        ApplicationSummaryReplayProjection,
        AgentPerformanceReplayProjection,
        ComplianceAuditReplayProjection,
    ]
    all_app_events = await _load_application_events(store, application_id)
    if not all_app_events:
        raise TimeTravelError(f"No events found for application_id={application_id}")
    exam = examination_date.astimezone(UTC)

    def _visible_as_of(e: StoredEvent) -> bool:
        t_rec = e.recorded_at.astimezone(UTC)
        try:
            t_bus = _event_time(e)
        except Exception:
            t_bus = t_rec
        return t_rec <= exam or t_bus <= exam

    as_of_events = [e for e in all_app_events if _visible_as_of(e)]
    projection_states = ProjectionRunner().run(events=as_of_events, projection_factories=projection_factories)

    audit_stream = f"audit-loan-{application_id}"
    audit_events = await store.load_stream(audit_stream)
    audit_checks = [e for e in audit_events if e.event_type == "AuditIntegrityCheckRun"]
    chain_valid, tamper_detected, last_hash, verified_count = verify_audit_chain(
        all_app_events,
        audit_checks,
    )
    integrity_hash = _integrity_hash(all_app_events[verified_count:], last_hash or "")

    narrative = [line for line in (_narrative_sentence(event) for event in as_of_events) if line]
    pkg = {
        "package_version": "1.0",
        "application_id": application_id,
        "examination_date": examination_date.astimezone(UTC).isoformat(),
        "event_stream": [_event_to_raw(e) for e in all_app_events],
        "projection_states_as_of_examination_date": projection_states,
        "audit_integrity": {
            "chain_valid": chain_valid,
            "tamper_detected": tamper_detected,
            "latest_previous_hash": last_hash,
            "computed_integrity_hash": integrity_hash,
            "hash_algorithm": "sha256",
            "verified_events_count": len(all_app_events),
            "audit_checkpoint_event_count": verified_count,
        },
        "narrative": narrative,
        "causal_links": _causal_links(all_app_events),
        "ai_decision_traceability": _extract_ai_trace(all_app_events),
    }
    canonical = json.dumps(pkg, sort_keys=True, separators=(",", ":"), default=_json_default)
    pkg["package_sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(pkg, f, sort_keys=True, indent=2, default=_json_default)
            f.write("\n")
    return pkg


def verify_regulatory_package(package: dict[str, Any]) -> tuple[bool, str]:
    """Recompute ``package_sha256`` from every field except ``package_sha256`` (receiver-side check).

    Returns ``(True, "")`` on match, else ``(False, reason)``. Does not mutate *package*.
    """
    claimed = package.get("package_sha256")
    if not isinstance(claimed, str) or len(claimed) != 64:
        return False, "package_sha256 must be a 64-char hex string"
    body = {k: v for k, v in package.items() if k != "package_sha256"}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), default=_json_default)
    actual = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if actual != claimed:
        return False, "package_sha256 does not match canonical body"
    return True, ""
