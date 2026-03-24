"""Counterfactual: first CreditAnalysisCompleted as MEDIUM-like → synthetic HIGH (bonus examination scenario)."""
from __future__ import annotations

import copy
from typing import Any

from event_store import EventStore, InMemoryEventStore
from what_if.projector import (
    ApplicationSummaryReplayProjection,
    ResultComparator,
    TimeTravelError,
    WhatIfResult,
    run_what_if,
)

StoreT = EventStore | InMemoryEventStore


def _tier_from_credit_payload(payload: dict[str, Any]) -> str | None:
    d = payload.get("decision")
    if isinstance(d, dict) and d.get("risk_tier") is not None:
        return str(d.get("risk_tier")).strip().upper()
    rt = payload.get("risk_tier")
    return str(rt).strip().upper() if rt is not None else None


def bump_credit_payload_medium_to_high(payload: dict[str, Any], *, limit_scale: float = 0.55) -> dict[str, Any]:
    """Deep-copy credit payload; set tier HIGH; tighten recommended_limit (simple policy knob for demo)."""
    out = copy.deepcopy(payload)
    dec = out.get("decision")
    if not isinstance(dec, dict):
        dec = {}
        out["decision"] = dec
    dec["risk_tier"] = "HIGH"
    raw = dec.get("recommended_limit_usd") or out.get("recommended_limit_usd")
    if raw is not None:
        try:
            v = float(str(raw))
            nv = max(1000.0, round(v * float(limit_scale), 2))
            s = str(nv)
            dec["recommended_limit_usd"] = s
            out["recommended_limit_usd"] = s
        except ValueError:
            pass
    out["risk_tier"] = "HIGH"
    return out


async def run_credit_medium_to_high_what_if(
    store: StoreT,
    application_id: str,
    *,
    limit_scale: float = 0.55,
) -> dict[str, Any]:
    """
    Branch at the first CreditAnalysisCompleted; inject HIGH tier + tighter limit.
    Returns structured examiner payload (never writes to the store).
    """
    aid = application_id.strip()
    from what_if.projector import _load_application_events

    app_events = await _load_application_events(store, aid)
    branch = next((e for e in app_events if e.event_type == "CreditAnalysisCompleted"), None)
    if branch is None:
        raise TimeTravelError(f"No CreditAnalysisCompleted for application_id={aid!r}")
    tier = _tier_from_credit_payload(dict(branch.payload))
    if tier == "HIGH":
        raise TimeTravelError(
            f"Credit tier is already HIGH for {aid!r}; pick an application whose credit analysis is MEDIUM (or lower) for this demo."
        )
    bumped = bump_credit_payload_medium_to_high(dict(branch.payload), limit_scale=limit_scale)
    result = await run_what_if(
        store,
        aid,
        "CreditAnalysisCompleted",
        [{"event_type": "CreditAnalysisCompleted", "event_version": int(branch.event_version), "payload": bumped}],
        projections=[ApplicationSummaryReplayProjection],
    )
    return package_medium_to_high_result(result, tier_before=tier)


def package_medium_to_high_result(result: WhatIfResult, *, tier_before: str | None) -> dict[str, Any]:
    real_s = result.real_outcome["application_summary"]
    cf_s = result.counterfactual_outcome["application_summary"]
    diff = ResultComparator().compare(real_s, cf_s)
    excluded = [d for d in result.divergence_events if d.reason == "causally_dependent_excluded"]
    decision_dropped = any(e.event_type == "DecisionGenerated" for e in excluded)
    approved_dropped = any(e.event_type == "ApplicationApproved" for e in excluded)
    notes: list[str] = [
        "Real timeline replays the committed stream. Counterfactual replaces the first CreditAnalysisCompleted cell "
        "with a synthetic HIGH-tier payload (tighter limit). Events causally chained from the original credit cell are "
        "excluded so stale orchestration is not replayed on top of the alternate credit outcome.",
    ]
    stale_warning: str | None = None
    if real_s.get("decision") and not decision_dropped:
        stale_warning = (
            "Original DecisionGenerated (and any binding) is still replayed after the counterfactual credit event "
            "because no metadata.causation_id chain was found from that decision to the credit event. "
            "For audit-grade counterfactuals, writers should link downstream decisions to their upstream credit event_id."
        )
        notes.append(stale_warning)

    return {
        "tier_before": tier_before,
        "branch_event_id": result.branch_event_id,
        "real_application_summary": real_s,
        "counterfactual_application_summary": cf_s,
        "application_summary_field_diff": diff,
        "divergence_events": [
            {
                "reason": d.reason,
                "event_id": d.event_id,
                "event_type": d.event_type,
                "stream_id": d.stream_id,
            }
            for d in result.divergence_events
        ],
        "causal_policy": {
            "decision_events_excluded": decision_dropped,
            "application_approved_excluded": approved_dropped,
            "dependent_events_dropped_count": len(excluded),
        },
        "examiner_notes": notes,
        "stale_downstream_warning": stale_warning,
    }
