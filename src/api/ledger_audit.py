"""
Staff HTTP surface for regulatory examination flows (same primitives as MCP / time_travel).

URI shapes from the challenge (e.g. ledger://applications/{id}/compliance?as_of=) are mirrored
as GET query parameters here so the portal and curl can run the demo without a separate demo app.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from append_concurrency_proof import (
    loan_append_concurrency_proof_plan_payload,
    reconcile_stream_head_version,
    run_loan_append_concurrency_proof,
)
from event_store import EventStore, coerce_jsonb_dict
from integrity.gas_town import reconstruct_agent_context
from gas_town_demo import append_gas_town_crash_demo
from integrity import run_integrity_check
from navigator_query import parse_navigator_decision_history_query
from regulatory.package import generate_regulatory_package
from what_if.projector import TimeTravelError, compliance_state_as_of_from_events, run_what_if
from what_if_credit_high import run_credit_medium_to_high_what_if

router = APIRouter(tags=["ledger-audit"])


def _store(request: Request) -> EventStore:
    s = getattr(request.app.state, "store", None)
    if s is None:
        raise HTTPException(503, "Event store not initialized")
    return s


@router.get("/api/applications/{application_id}/audit/decision-history")
async def decision_history_package(
    request: Request,
    application_id: str,
    examination_at: datetime | None = None,
) -> dict[str, Any]:
    """Full event narrative, projections as-of date, integrity chain fields, AI trace — under one round-trip."""
    store = _store(request)
    at = examination_at if examination_at is not None else datetime.now(UTC)
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    t0 = time.perf_counter()
    try:
        pkg = await generate_regulatory_package(store, application_id, at)
    except TimeTravelError as e:
        raise HTTPException(404, str(e)) from e
    ms = (time.perf_counter() - t0) * 1000.0
    return {"ok": True, "duration_ms": round(ms, 2), "examination_at": at.astimezone(UTC).isoformat(), "package": pkg}


class NavigatorDecisionHistoryBody(BaseModel):
    query: str = Field(..., min_length=5, max_length=8000)


@router.post("/api/navigator/decision-history")
async def navigator_decision_history_nl(
    request: Request,
    body: NavigatorDecisionHistoryBody,
) -> dict[str, Any]:
    """Parse natural-language staff request, then return the same regulatory package as GET decision-history."""
    outcome = parse_navigator_decision_history_query(body.query)
    if outcome.error:
        raise HTTPException(400, outcome.error)
    app_id = outcome.application_id
    assert app_id is not None
    store = _store(request)
    at = outcome.examination_at if outcome.examination_at is not None else datetime.now(UTC)
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    t0 = time.perf_counter()
    try:
        pkg = await generate_regulatory_package(store, app_id, at)
    except TimeTravelError as e:
        raise HTTPException(404, str(e)) from e
    ms = (time.perf_counter() - t0) * 1000.0
    return {
        "ok": True,
        "parsed": {
            "application_id": app_id,
            "intent_matched": outcome.intent_matched,
            "examination_at_from_query": outcome.examination_at.isoformat() if outcome.examination_at else None,
            "signals": outcome.signals,
        },
        "duration_ms": round(ms, 2),
        "examination_at": at.astimezone(UTC).isoformat(),
        "package": pkg,
    }


@router.get("/api/applications/{application_id}/compliance")
async def compliance_as_of(
    request: Request,
    application_id: str,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """
    Temporal compliance: matches MCP resource ``ledger://applications/{id}/compliance?as_of=``.
    Uses ``compliance_audit_snapshots`` when present; otherwise event replay (same as examination replay).
    """
    store = _store(request)
    if as_of is None:
        raise HTTPException(400, "Query parameter as_of is required (ISO-8601 timestamp)")
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=UTC)
    pool = store._require_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                SELECT snapshot_payload
                FROM compliance_audit_snapshots
                WHERE application_id = $1 AND snapshot_at <= $2
                ORDER BY snapshot_at DESC
                LIMIT 1
                """,
                application_id,
                as_of,
            )
        except asyncpg.UndefinedTableError:
            row = None
    if row:
        data = coerce_jsonb_dict(row["snapshot_payload"])
        return {"ok": True, "source": "compliance_audit_snapshots", "as_of": as_of.isoformat(), "data": data}
    try:
        data = await compliance_state_as_of_from_events(store, application_id, as_of)
    except TimeTravelError as e:
        raise HTTPException(404, str(e)) from e
    return {"ok": True, "source": "event_replay", "as_of": as_of.isoformat(), "data": data}


@router.get("/api/applications/{application_id}/compliance/current")
async def compliance_current(request: Request, application_id: str) -> dict[str, Any]:
    store = _store(request)
    pool = store._require_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM compliance_audit_current WHERE application_id = $1",
                application_id,
            )
    except asyncpg.UndefinedTableError:
        row = None
    if row:
        return {"ok": True, "source": "compliance_audit_current", "data": dict(row)}
    now = datetime.now(UTC)
    try:
        data = await compliance_state_as_of_from_events(store, application_id, now)
    except TimeTravelError as e:
        raise HTTPException(404, str(e)) from e
    return {"ok": True, "source": "event_replay", "data": data}


@router.get("/api/applications/{application_id}/compliance/temporal-demo-hints")
async def compliance_temporal_demo_hints(request: Request, application_id: str) -> dict[str, Any]:
    """
    Week 5 Step 3 helper: reads ``events`` for this application and returns ISO ``as_of`` values that
    straddle the first ``ComplianceCheckCompleted`` so the UI can demonstrate temporal vs current state
    using the same contract as ``ledger://applications/{id}/compliance?as_of=``.
    """
    store = _store(request)
    aid = application_id.strip()
    pool = store._require_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT MIN(recorded_at) AS first_cc_at
            FROM events
            WHERE event_type = 'ComplianceCheckCompleted'
              AND (payload->>'application_id') = $1
            """,
            aid,
        )
        snapshots = False
        try:
            reg = await conn.fetchval("SELECT to_regclass('public.compliance_audit_snapshots')")
            snapshots = reg is not None
        except asyncpg.UndefinedTableError:
            snapshots = False
    if not row or row["first_cc_at"] is None:
        raise HTTPException(
            404,
            f"No ComplianceCheckCompleted in database for application_id={aid!r}. "
            "Run the workflow for this application (or pick another id from your ledger).",
        )
    first_cc = row["first_cc_at"]
    if first_cc.tzinfo is None:
        first_cc = first_cc.replace(tzinfo=UTC)
    # Keep "before" after typical submit but before completion (500ms prior to first completion).
    before = (first_cc - timedelta(milliseconds=500)).isoformat()
    after = (first_cc + timedelta(milliseconds=500)).isoformat()
    return {
        "ok": True,
        "week5_prerequisite_step_3": (
            "Temporal Compliance Query: Query ledger://applications/{id}/compliance?as_of={timestamp} "
            "for a past point in time. Show the compliance state as it existed at that moment, distinct from the current state."
        ),
        "application_id": aid,
        "ledger_resource_template": "ledger://applications/{application_id}/compliance?as_of={iso8601}",
        "http_equivalent": (
            f"GET /api/applications/{aid}/compliance?as_of=<ISO-8601> — mirrors the MCP resource query string."
        ),
        "compliance_snapshot_table_available": snapshots,
        "temporal_backend_behavior": (
            "If compliance_audit_snapshots exists and has a row at or before as_of, return that payload; "
            "otherwise replay ComplianceAuditReplayProjection on all application events with audit-time ≤ as_of."
        ),
        "first_compliance_check_completed_recorded_at_utc": first_cc.isoformat(),
        "suggested_as_of_before_first_completion_utc": before,
        "suggested_as_of_after_first_completion_utc": after,
    }


@router.get("/api/applications/{application_id}/ledger/append-concurrency-proof/plan")
async def loan_append_concurrency_proof_plan(
    request: Request,
    application_id: str,
) -> dict[str, Any]:
    """Live ``event_streams.current_version`` / repair + narrative for the two-writer ``EventStore.append`` race."""
    store = _store(request)
    aid = application_id.strip()
    stream_id = f"loan-{aid}"
    v, repaired = await reconcile_stream_head_version(store, stream_id)
    if v < 0:
        raise HTTPException(
            404,
            f"No loan stream in database for application_id={aid!r} (no events for {stream_id}).",
        )
    return loan_append_concurrency_proof_plan_payload(aid, stream_id, v, repaired)


@router.post("/api/applications/{application_id}/ledger/append-concurrency-proof")
async def loan_append_concurrency_proof_run(request: Request, application_id: str) -> dict[str, Any]:
    """
    Operational check: same PostgreSQL CAS as all writers — two concurrent ``append`` calls share one
    ``expected_version``; one succeeds, one ``OptimisticConcurrencyError``, then a single reload+retry.
    Appends real ``FraudScreeningRequested`` events (production loan-stream type).
    """
    store = _store(request)
    try:
        return await run_loan_append_concurrency_proof(store, application_id)
    except ValueError as e:
        raise HTTPException(404, str(e)) from e


@router.get("/api/streams/{stream_id}/events/{stream_position}/upcast-proof")
async def upcast_proof(request: Request, stream_id: str, stream_position: int) -> dict[str, Any]:
    """DB row (immutable) vs ``load_stream`` (upcasted) for the same cell."""
    store = _store(request)
    pool = store._require_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT event_id, event_type, event_version, payload, metadata, recorded_at
            FROM events
            WHERE stream_id = $1 AND stream_position = $2
            """,
            stream_id,
            stream_position,
        )
    if row is None:
        raise HTTPException(404, "event not found")
    raw_payload = coerce_jsonb_dict(row["payload"])
    raw_ver = int(row["event_version"])
    loaded = await store.load_stream(stream_id)
    match = next((e for e in loaded if e.stream_position == stream_position), None)
    if match is None:
        raise HTTPException(500, "load_stream missing position")
    async with pool.acquire() as conn:
        row_after = await conn.fetchrow(
            """
            SELECT event_version, payload FROM events
            WHERE stream_id = $1 AND stream_position = $2
            """,
            stream_id,
            stream_position,
        )
    db_after = coerce_jsonb_dict(row_after["payload"]) if row_after else {}
    return {
        "ok": True,
        "stream_id": stream_id,
        "stream_position": stream_position,
        "database_row": {
            "event_type": row["event_type"],
            "event_version": raw_ver,
            "payload": raw_payload,
        },
        "through_event_store": {
            "event_type": match.event_type,
            "event_version": match.event_version,
            "payload": dict(match.payload),
        },
        "stored_payload_bytes_unchanged_after_read": db_after == raw_payload,
        "stored_event_version_unchanged": int(row_after["event_version"]) == raw_ver,
    }


class ReconstructAgentBody(BaseModel):
    agent_id: str = Field(..., examples=["document_processing"])
    session_id: str = Field(..., examples=["sess-doc-abc"])


class GasTownCrashDemoBody(BaseModel):
    application_id: str = Field(
        "APEX-GAS-DEMO",
        description="application_id in agent payloads (any string; not required to exist as a loan stream).",
    )
    agent_id: str = Field("credit_analysis", examples=["credit_analysis"])


@router.post("/api/ledger/gas-town-crash-demo")
async def gas_town_crash_demo_seed(request: Request, body: GasTownCrashDemoBody | None = None) -> dict[str, Any]:
    """
    Week 5 Step 5: append a realistic agent-session sequence ending in ``AgentSessionFailed``,
    then call ``POST /api/ledger/reconstruct-agent-context`` with the returned ``agent_id`` / ``session_id``
    to prove recovery without in-memory process state.
    """
    store = _store(request)
    b = body if body is not None else GasTownCrashDemoBody()
    seeded = await append_gas_town_crash_demo(
        store,
        application_id=b.application_id.strip() or "APEX-GAS-DEMO",
        agent_id=b.agent_id.strip() or "credit_analysis",
    )
    return {"ok": True, **seeded}


@router.get("/api/ledger/gas-town-demo-hint")
async def gas_town_demo_hint() -> dict[str, Any]:
    """Static copy for challengers: curl + UI flow for Step 5."""
    return {
        "ok": True,
        "step": (
            "Gas Town recovery: persist agent session to the event store, lose process memory (restart API or new client), "
            "then reconstruct context from stream ``agent-{agent_id}-{session_id}`` via reconstruct_agent_context()."
        ),
        "seed_http": "POST /api/ledger/gas-town-crash-demo  body: {\"application_id\":\"APEX-9756\",\"agent_id\":\"credit_analysis\"}",
        "reconstruct_http": "POST /api/ledger/reconstruct-agent-context  body: {\"agent_id\":\"…\",\"session_id\":\"…\"}  (use values returned by seed)",
        "expected_signals": [
            "last_event_position matches events appended (5 for the bundled scenario).",
            "context_text includes verbatim tail events (e.g. AgentSessionFailed) and summary of earlier types.",
            "pending_work lists reconciliation items (e.g. DecisionGenerated ack) and recovery from AgentSessionFailed.",
            "session_health_status NEEDS_RECONCILIATION when work is pending and session not completed.",
        ],
    }


@router.post("/api/ledger/reconstruct-agent-context")
async def reconstruct_agent(request: Request, body: ReconstructAgentBody) -> dict[str, Any]:
    store = _store(request)
    ctx = await reconstruct_agent_context(store, body.agent_id, body.session_id)
    stream_id = f"agent-{body.agent_id}-{body.session_id}"
    tail = await store.load_stream(stream_id)
    tail_types = [e.event_type for e in tail[-5:]] if tail else []
    return {
        "ok": True,
        "stream_id": stream_id,
        "session_id": body.session_id,
        "agent_id": body.agent_id,
        "last_event_position": ctx.last_event_position,
        "session_health_status": ctx.session_health_status,
        "pending_work": ctx.pending_work,
        "context_text": ctx.context_text,
        "tail_event_types": tail_types,
        "persisted_event_count": len(tail),
    }


class WhatIfBody(BaseModel):
    application_id: str
    branch_at_event_type: str = "CreditAnalysisCompleted"
    counterfactual_events: list[dict[str, Any]] = Field(
        ...,
        description="Event dicts with event_type, event_version, payload",
    )


@router.post("/api/ledger/what-if")
async def what_if(request: Request, body: WhatIfBody) -> dict[str, Any]:
    store = _store(request)
    try:
        result = await run_what_if(
            store,
            body.application_id,
            body.branch_at_event_type,
            body.counterfactual_events,
        )
    except TimeTravelError as e:
        raise HTTPException(400, str(e)) from e
    return {
        "ok": True,
        "branch_event_id": result.branch_event_id,
        "real_outcome": result.real_outcome,
        "counterfactual_outcome": result.counterfactual_outcome,
        "divergence_events": [
            {
                "reason": d.reason,
                "event_id": d.event_id,
                "event_type": d.event_type,
                "stream_id": d.stream_id,
            }
            for d in result.divergence_events
        ],
    }


@router.get("/api/applications/{application_id}/ledger/what-if-medium-to-high/hint")
async def what_if_medium_to_high_hint(application_id: str) -> dict[str, Any]:
    """Step 6: example id with MEDIUM credit in seed data (~APEX-0020)."""
    sample = "APEX-0020"
    aid = application_id.strip()
    return {
        "ok": True,
        "application_id_param": aid,
        "sample_application_id_medium_credit": sample,
        "run": f"POST /api/applications/{sample}/ledger/what-if-medium-to-high",
        "narrative": (
            "Replays projections with the first CreditAnalysisCompleted replaced by a synthetic HIGH tier and a tighter "
            "recommended limit. Downstream events that declare causation to the original credit cell are omitted so the "
            "orchestrator decision is not inherited from the real timeline."
        ),
    }


@router.post("/api/applications/{application_id}/ledger/what-if-medium-to-high")
async def what_if_medium_to_high(request: Request, application_id: str) -> dict[str, Any]:
    """Step 6 bonus: MEDIUM→HIGH credit counterfactual; read-only; compares application_summary only."""
    store = _store(request)
    try:
        payload = await run_credit_medium_to_high_what_if(store, application_id)
    except TimeTravelError as e:
        raise HTTPException(400, str(e)) from e
    return {"ok": True, **payload}


class IntegrityBody(BaseModel):
    entity_type: str = Field("loan", description="Stream prefix, e.g. loan")
    entity_id: str = Field(..., description="Application id without loan- prefix, e.g. APEX-0001")


@router.post("/api/ledger/integrity-check")
async def integrity_check(request: Request, body: IntegrityBody) -> dict[str, Any]:
    store = _store(request)
    result = await run_integrity_check(store, body.entity_type, body.entity_id)
    return {
        "ok": True,
        "check_result": {
            "events_verified": result.events_verified,
            "integrity_hash": result.integrity_hash,
            "previous_hash": result.previous_hash,
            "tamper_detected": result.tamper_detected,
        },
        "chain_valid": result.chain_valid,
    }
