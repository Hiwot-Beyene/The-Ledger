"""
Staff / portal HTTP API.

Run: ``uvicorn src.api.app:app --reload --port 8080`` (set ``DATABASE_URL``).

- ``GET /api/registry/companies`` — read-only applicant registry (CRM boundary).
- ``GET /api/applications/pipeline`` — from ``application_summary`` when present, else derived from ``events`` (loan streams).
- ``GET /api/applications/{application_id}/summary`` — dossier + orchestrator vs human vs binding outcome.
- ``POST /api/applications`` — append ``ApplicationSubmitted`` (requires existing ``applicant_id`` in registry).
- ``GET /api/applications/{application_id}/audit/decision-history`` — regulatory package (events, projections, integrity, AI trace).
- ``POST /api/navigator/decision-history`` — same package; ``query`` body is natural language (intent + application id).
- ``GET /api/applications/{application_id}/compliance`` — ``as_of`` query (snapshots or event replay); ``/compliance/current``; ``/compliance/temporal-demo-hints`` (DB-derived ``as_of`` suggestions for Week 5 Step 3).
- ``POST /api/applications/{application_id}/ledger/append-concurrency-proof`` — two-writer OCC on ``loan-{id}`` via ``EventStore.append`` (real ``FraudScreeningRequested``); ``GET .../append-concurrency-proof/plan`` reads ``event_streams.current_version``.
- ``GET /api/streams/{stream_id}/events/{stream_position}/upcast-proof`` — raw ``events`` row vs ``load_stream`` (Step 4: upcast + immutability).
- ``POST /api/ledger/gas-town-crash-demo`` — append five agent session events ending in ``AgentSessionFailed`` (Step 5 seed); ``GET /api/ledger/gas-town-demo-hint`` — curl copy.
- ``POST /api/ledger/reconstruct-agent-context`` — Gas Town session replay from ``agent-{agent_id}-{session_id}``.
- ``POST /api/applications/{application_id}/ledger/what-if-medium-to-high`` — bonus counterfactual (MEDIUM→HIGH credit); ``GET .../what-if-medium-to-high/hint``.
- ``POST /api/ledger/what-if`` — counterfactual projection replay.
- ``POST /api/ledger/integrity-check`` — rolling hash chain + ``AuditIntegrityCheckRun`` append.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

_SRC_ROOT = Path(__file__).resolve().parent.parent
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

_REPO_ROOT = _SRC_ROOT.parent
if load_dotenv:
    load_dotenv(_REPO_ROOT / ".env", override=False)

import asyncpg
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from commands.handlers import SubmitApplicationCommand, handle_submit_application
from event_store import EventStore, coerce_jsonb_dict
from models.events import (
    DocumentFormat,
    DocumentType,
    DomainError,
    LoanPurpose,
    DocumentUploaded,
)
from mcp.server import CallerContext, LedgerMCPService

from .ledger_audit import router as ledger_audit_router

_store: EventStore | None = None
_workflow_tasks: dict[str, asyncio.Task[dict[str, Any]] | None] = {}

def _start_workflow_task(application_id: str) -> None:
    """
    Start (or restart) the async workflow runner for an application.

    Important: background task exceptions are otherwise easy to miss; this
    function ensures they are logged to the terminal.
    """
    existing = _workflow_tasks.get(application_id)
    if existing is not None and not existing.done():
        existing.cancel()

    async def _runner() -> dict[str, Any]:
        try:
            return await _run_workflow(application_id)
        except asyncio.CancelledError:
            # Best-effort audit trail so UI doesn't hang silently.
            try:
                if _store is not None:
                    evt = {
                        "event_type": "DomainError",
                        "event_version": 1,
                        "payload": {
                            "application_id": application_id,
                            "session_id": f"workflow-{application_id}",
                            "node_name": "workflow_runner",
                            "document_id": None,
                            "code": "workflow_cancelled",
                            "message": "Workflow task was cancelled (likely server reload during dev).",
                            "details": {"application_id": application_id},
                            "recorded_at": datetime.now(UTC).isoformat(),
                        },
                    }
                    ver = await _store.stream_version(f"loan-{application_id}")
                    await _store.append(
                        stream_id=f"loan-{application_id}",
                        events=[evt],
                        expected_version=ver,
                        correlation_id=application_id,
                        causation_id=f"workflow-{application_id}",
                    )
            except Exception:
                pass
            raise
        except Exception as e:
            # Ensure we see background failures in the terminal.
            print(f"[workflow] ERROR application_id={application_id} err={type(e).__name__}: {e}", flush=True)
            raise

    task = asyncio.create_task(_runner())

    def _done(t: asyncio.Task) -> None:
        try:
            _ = t.result()
            print(f"[workflow] done application_id={application_id}", flush=True)
        except asyncio.CancelledError:
            print(f"[workflow] cancelled application_id={application_id}", flush=True)
        except Exception as e:
            print(f"[workflow] failed application_id={application_id} err={type(e).__name__}: {e}", flush=True)

    task.add_done_callback(_done)
    _workflow_tasks[application_id] = task


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _store
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is required for the staff API")
    _store = EventStore(url)
    await _store.connect()
    app.state.store = _store
    yield
    if _store:
        await _store.close()
        _store = None
        app.state.store = None


app = FastAPI(title="Apex Ledger Staff API", lifespan=_lifespan)
app.include_router(ledger_audit_router)

_origins_raw = os.environ.get("STAFF_API_CORS_ORIGINS", "*").strip()
_origins = _origins_raw.split(",") if _origins_raw else ["*"]
_allow_credentials = True
if any(o.strip() == "*" for o in _origins):
    # `Access-Control-Allow-Credentials: true` is not valid with wildcard origins.
    _allow_credentials = False
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in _origins if o.strip()],
    allow_credentials=_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _pool() -> asyncpg.Pool:
    if _store is None:
        raise HTTPException(503, "Event store not initialized")
    return _store._require_pool()


def _ui_stage(last_event_type: str | None) -> str:
    if not last_event_type:
        return "Intake"
    if last_event_type in ("ApplicationApproved", "ApplicationDeclined"):
        return "Complete"
    if last_event_type == "HumanReviewCompleted":
        return "Decision"
    if last_event_type in ("DecisionGenerated", "HumanReviewRequested", "DecisionRequested"):
        return "Decision"
    if last_event_type == "ComplianceCheckCompleted":
        return "Compliance"
    if last_event_type in ("ComplianceCheckRequested", "ComplianceCheckInitiated"):
        return "Compliance"
    if last_event_type == "FraudScreeningCompleted":
        return "Fraud"
    if last_event_type == "FraudScreeningRequested":
        return "Fraud"
    if last_event_type == "CreditAnalysisCompleted":
        return "Credit"
    if last_event_type == "CreditAnalysisRequested":
        return "Credit"
    if last_event_type in (
        "DocumentUploadRequested",
        "DocumentUploaded",
        "DocumentUploadFailed",
        "AgentSessionCompleted",
    ):
        return "Documents"
    if last_event_type == "ApplicationSubmitted":
        return "Intake"
    return "Intake"


def _fmt_usd(n: Any) -> str:
    if n is None:
        return "—"
    try:
        return f"${float(n):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _fmt_purpose(raw: str | None) -> str:
    if not raw:
        return "—"
    return str(raw).replace("_", " ").title()


def _fmt_updated(at: datetime | None) -> str:
    if at is None:
        return "—"
    if at.tzinfo is None:
        return at.strftime("%b %d, %H:%M")
    return at.astimezone().strftime("%b %d, %H:%M")


MILESTONE_TYPES = (
    "ApplicationSubmitted",
    "DocumentUploadRequested",
    "DocumentUploaded",
    "PackageReadyForAnalysis",
    "CreditAnalysisRequested",
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceCheckCompleted",
    "DecisionGenerated",
    "HumanReviewRequested",
    "HumanReviewCompleted",
    "ApplicationApproved",
    "ApplicationDeclined",
)

# Authoritative on loan-{id}; multi-stream milestone query can pick the wrong row for these types.
_LOAN_STREAM_PRIORITY_TYPES = (
    "DecisionGenerated",
    "HumanReviewCompleted",
    "ApplicationApproved",
    "ApplicationDeclined",
)


async def _merge_latest_loan_milestones(
    conn: Any, loan_stream: str, by_type: dict[str, dict[str, Any]]
) -> None:
    for et in _LOAN_STREAM_PRIORITY_TYPES:
        row = await conn.fetchrow(
            """
            SELECT payload, recorded_at
            FROM events
            WHERE stream_id = $1 AND event_type = $2
            ORDER BY stream_position DESC
            LIMIT 1
            """,
            loan_stream,
            et,
        )
        if row:
            by_type[et] = {
                "payload": coerce_jsonb_dict(row["payload"]),
                "recorded_at": row["recorded_at"].isoformat() if row["recorded_at"] else None,
            }

EVENT_LABELS: dict[str, str] = {
    "ApplicationSubmitted": "Application captured",
    "DocumentUploadRequested": "Documents requested",
    "DocumentUploaded": "Document uploaded",
    "PackageReadyForAnalysis": "Document package ready for analysis",
    "CreditAnalysisCompleted": "Credit analysis",
    "FraudScreeningCompleted": "Fraud screening",
    "ComplianceCheckCompleted": "Compliance verdict",
    "DecisionGenerated": "Orchestrator recommendation (model output)",
    "HumanReviewRequested": "Human review requested (low confidence or REFER)",
    "HumanReviewCompleted": "Human review recorded",
    "ApplicationApproved": "Binding approval",
    "ApplicationDeclined": "Binding decline",
}

DUE_DILIGENCE_EVENTS = (
    "CreditAnalysisCompleted",
    "FraudScreeningCompleted",
    "ComplianceCheckCompleted",
)


def _iso_utc(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def _hitl_confidence_policy(by_type: dict[str, dict[str, Any]]) -> tuple[float, bool]:
    """HITL when confidence is below floor or automatic binding is disabled (matches orchestrator write_output)."""
    threshold = float(os.getenv("LEDGER_HITL_CONFIDENCE_THRESHOLD", "0.6") or "0.6")
    dg = by_type.get("DecisionGenerated")
    if not dg:
        return threshold, False
    pl = dg.get("payload") or {}
    try:
        c = float(pl.get("confidence_score") or pl.get("confidence") or 0)
    except (TypeError, ValueError):
        c = 0.0
    auto_off = os.getenv("LEDGER_AUTO_BIND_HIGH_CONFIDENCE", "1").strip().lower() in (
        "0",
        "false",
        "no",
    )
    need_hitl = (c < threshold) or auto_off
    return threshold, need_hitl


def _decision_path(by_type: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Ordered regulatory decision path: each node has status + timestamps from the loan stream."""

    def rec_at(et: str) -> str | None:
        row = by_type.get(et)
        return row.get("recorded_at") if row else None

    sub = by_type.get("ApplicationSubmitted")
    dup_req = by_type.get("DocumentUploadRequested")
    doc_up = by_type.get("DocumentUploaded")
    credit_started = "CreditAnalysisRequested" in by_type or "CreditAnalysisCompleted" in by_type
    if not dup_req:
        docs_status = "blocked"
    elif credit_started:
        docs_status = "completed"
    elif doc_up:
        docs_status = "in_progress"
    else:
        docs_status = "pending"

    checks = [
        {
            "event_type": et,
            "label": EVENT_LABELS.get(et, et),
            "completed": et in by_type,
            "recorded_at": rec_at(et),
        }
        for et in DUE_DILIGENCE_EVENTS
    ]
    done_n = sum(1 for c in checks if c["completed"])
    if not sub:
        dd_status = "blocked"
    elif done_n == len(DUE_DILIGENCE_EVENTS):
        dd_status = "completed"
    elif done_n > 0:
        dd_status = "in_progress"
    else:
        dd_status = "pending"

    dg = by_type.get("DecisionGenerated")
    hr = by_type.get("HumanReviewCompleted")
    hitl_threshold, hitl_required = _hitl_confidence_policy(by_type)

    has_binding = "ApplicationApproved" in by_type or "ApplicationDeclined" in by_type
    if not dg:
        if has_binding:
            orch_status = "skipped"
        elif not sub or not credit_started:
            orch_status = "blocked"
        elif dd_status == "completed":
            orch_status = "pending"
        else:
            orch_status = "blocked"
    else:
        orch_status = "completed"

    # Only "completed" when HITL was required; high-confidence auto path shows skipped even if HR event exists.
    if hr and hitl_required:
        hr_status = "completed"
    elif hr and not hitl_required:
        hr_status = "skipped"
    elif has_binding:
        hr_status = "skipped"
    elif not dg:
        hr_status = "blocked"
    elif not hitl_required:
        hr_status = "skipped"
    else:
        hr_status = "pending"

    dg_payload = (dg or {}).get("payload") or {}
    dg_rec = str(dg_payload.get("recommendation", "") or "").upper()

    if "ApplicationApproved" in by_type:
        bind_status = "completed"
        bind_outcome = "approved"
    elif "ApplicationDeclined" in by_type:
        bind_status = "completed"
        bind_outcome = "declined"
    elif dg and not hitl_required and not has_binding:
        # Auto-binding (including REFER→approve|decline) runs with orchestrator; brief window before events land.
        bind_status = "in_progress"
        bind_outcome = None
    elif dg or (hr and hitl_required):
        bind_status = "pending"
        bind_outcome = None
    else:
        bind_status = "blocked"
        bind_outcome = None

    return [
        {
            "step_id": "submission",
            "label": "Application submission",
            "description": "Capture applicant, amount, and purpose on the loan aggregate stream.",
            "status": "completed" if sub else "pending",
            "recorded_at": rec_at("ApplicationSubmitted"),
            "event_type": "ApplicationSubmitted",
        },
        {
            "step_id": "documents",
            "label": "Document intake and package processing",
            "description": "System requests required artifacts; document agent processes uploads and emits CreditAnalysisRequested when ready.",
            "status": docs_status,
            "recorded_at": rec_at("CreditAnalysisRequested") or rec_at("DocumentUploaded") or rec_at("DocumentUploadRequested"),
            "event_type": "CreditAnalysisRequested" if credit_started else "DocumentUploadRequested",
        },
        {
            "step_id": "due_diligence",
            "label": "Credit, fraud, and compliance checks",
            "description": "All three domain verdicts must exist before the model may recommend.",
            "status": dd_status,
            "recorded_at": None,
            "event_type": None,
            "checks": checks,
        },
        {
            "step_id": "orchestrator",
            "label": "Orchestrator (non-binding recommendation)",
            "description": "Model output; not legally binding until staff binding events.",
            "status": orch_status,
            "recorded_at": rec_at("DecisionGenerated"),
            "event_type": "DecisionGenerated",
            "detail": {
                "recommendation": dg_payload.get("recommendation"),
                "confidence": dg_payload.get("confidence_score") or dg_payload.get("confidence"),
                "summary": dg_payload.get("executive_summary") or dg_payload.get("summary"),
                "key_risks": dg_payload.get("key_risks") or [],
                "conditions": dg_payload.get("conditions") or [],
            },
        },
        {
            "step_id": "human_review",
            "label": "Human in the loop",
            "description": (
                f"Queued when confidence < {hitl_threshold} or LEDGER_AUTO_BIND_HIGH_CONFIDENCE is off. "
                "With auto-binding on and confidence at/above the floor, APPROVE/DECLINE/REFER can bind without this step. "
                "Staff binding records HumanReviewCompleted + ApplicationApproved/Declined."
            ),
            "status": hr_status,
            "recorded_at": (
                rec_at("HumanReviewCompleted")
                if hr_status == "completed"
                else (
                    rec_at("HumanReviewRequested")
                    if hr_status == "pending" and "HumanReviewRequested" in by_type
                    else None
                )
            ),
            "event_type": (
                "HumanReviewCompleted"
                if hr_status == "completed"
                else (
                    "HumanReviewRequested"
                    if hr_status == "pending" and "HumanReviewRequested" in by_type
                    else None
                )
            ),
            "hitl_required": hitl_required,
            "hitl_threshold": hitl_threshold,
        },
        {
            "step_id": "binding",
            "label": "Binding outcome",
            "description": "ApplicationApproved or ApplicationDeclined — authoritative decision.",
            "status": bind_status,
            "recorded_at": rec_at("ApplicationApproved") or rec_at("ApplicationDeclined"),
            "event_type": "ApplicationApproved" if "ApplicationApproved" in by_type else ("ApplicationDeclined" if "ApplicationDeclined" in by_type else None),
            "outcome": bind_outcome,
        },
    ]

_PIPELINE_SQL_FROM_SUMMARY = """
                SELECT s.application_id,
                       s.applicant_id,
                       s.requested_amount_usd,
                       s.last_event_type,
                       s.last_event_at,
                       c.name AS company_name,
                       sub.purpose AS loan_purpose
                FROM application_summary s
                LEFT JOIN applicant_registry.companies c ON c.company_id = s.applicant_id
                LEFT JOIN LATERAL (
                    SELECT e.payload->>'loan_purpose' AS purpose
                    FROM events e
                    WHERE e.stream_id = ('loan-' || s.application_id)
                      AND e.event_type = 'ApplicationSubmitted'
                    ORDER BY e.stream_position ASC
                    LIMIT 1
                ) sub ON true
                ORDER BY s.application_id
"""

_PIPELINE_SQL_FROM_EVENTS = """
                WITH loan_streams AS (
                    SELECT DISTINCT stream_id FROM events WHERE stream_id LIKE 'loan-%'
                ),
                first_sub AS (
                    SELECT DISTINCT ON (e.stream_id)
                        e.stream_id,
                        e.payload->>'applicant_id' AS applicant_id,
                        (e.payload->>'requested_amount_usd')::numeric AS requested_amount_usd,
                        e.payload->>'loan_purpose' AS loan_purpose
                    FROM events e
                    WHERE e.event_type = 'ApplicationSubmitted'
                    ORDER BY e.stream_id, e.stream_position ASC
                ),
                last_ev AS (
                    SELECT DISTINCT ON (e.stream_id)
                        e.stream_id,
                        e.event_type AS last_event_type,
                        e.recorded_at AS last_event_at
                    FROM events e
                    INNER JOIN loan_streams ls ON ls.stream_id = e.stream_id
                    ORDER BY e.stream_id, e.stream_position DESC
                )
                SELECT replace(ls.stream_id, 'loan-', '') AS application_id,
                       fs.applicant_id,
                       fs.requested_amount_usd,
                       le.last_event_type,
                       le.last_event_at,
                       c.name AS company_name,
                       fs.loan_purpose
                FROM loan_streams ls
                INNER JOIN first_sub fs ON fs.stream_id = ls.stream_id
                INNER JOIN last_ev le ON le.stream_id = ls.stream_id
                LEFT JOIN applicant_registry.companies c ON c.company_id = fs.applicant_id
                ORDER BY application_id
"""


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/registry/companies")
async def list_registry_companies() -> list[dict[str, Any]]:
    pool = _pool()
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT company_id, name, industry, jurisdiction, legal_type, trajectory, risk_segment
                FROM applicant_registry.companies
                ORDER BY company_id
                """
            )
        except asyncpg.UndefinedTableError as e:
            raise HTTPException(503, "applicant_registry not provisioned — run datagen/generate_all.py") from e
        flags = await conn.fetch(
            """
            SELECT company_id, flag_type, severity, is_active, added_date::text, note
            FROM applicant_registry.compliance_flags
            WHERE is_active = true
            """
        )
    by_company: dict[str, list[dict[str, Any]]] = {}
    for f in flags:
        cid = f["company_id"]
        by_company.setdefault(cid, []).append(
            {
                "flag_type": f["flag_type"],
                "severity": f["severity"],
                "is_active": f["is_active"],
                "added_date": f["added_date"],
                "note": f["note"],
            }
        )
    return [
        {
            "company_id": r["company_id"],
            "name": r["name"],
            "industry": r["industry"],
            "jurisdiction": r["jurisdiction"],
            "legal_type": r["legal_type"],
            "trajectory": r["trajectory"],
            "risk_segment": r["risk_segment"],
            "compliance_flags": by_company.get(r["company_id"], []),
        }
        for r in rows
    ]


@app.get("/api/applications/pipeline")
async def applications_pipeline() -> dict[str, Any]:
    pool = _pool()
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(_PIPELINE_SQL_FROM_SUMMARY)
        except asyncpg.UndefinedTableError:
            try:
                rows = await conn.fetch(_PIPELINE_SQL_FROM_EVENTS)
            except asyncpg.UndefinedTableError as e:
                raise HTTPException(503, "events not provisioned — run datagen or ledger migration") from e

    applications = []
    for r in rows:
        let = r["last_event_type"]
        applications.append(
            {
                "id": r["application_id"],
                "applicantId": r["applicant_id"],
                "companyName": r["company_name"] or r["applicant_id"],
                "amount": _fmt_usd(r["requested_amount_usd"]),
                "purpose": _fmt_purpose(r["loan_purpose"]),
                "state": _ui_stage(let),
                "updated": _fmt_updated(r["last_event_at"]),
                "lastEventAt": r["last_event_at"].isoformat() if r["last_event_at"] else "",
            }
        )
    return {"applications": applications}


@app.get("/api/applications/{application_id}/summary")
async def application_summary(application_id: str) -> dict[str, Any]:
    pool = _pool()
    loan_stream = f"loan-{application_id}"
    docpkg_stream = f"docpkg-{application_id}"
    credit_stream = f"credit-{application_id}"
    fraud_stream = f"fraud-{application_id}"
    compliance_stream = f"compliance-{application_id}"
    workflow_streams = [loan_stream, docpkg_stream, credit_stream, fraud_stream, compliance_stream]
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                SELECT s.*, c.name AS company_name
                FROM application_summary s
                LEFT JOIN applicant_registry.companies c ON c.company_id = s.applicant_id
                WHERE s.application_id = $1
                """,
                application_id,
            )
        except asyncpg.UndefinedTableError:
            row = None
        if row:
            base: dict[str, Any] = dict(row)
        else:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM events WHERE stream_id = $1)",
                loan_stream,
            )
            if not exists:
                raise HTTPException(404, "Unknown application_id")
            last_ev = await conn.fetchrow(
                """
                SELECT event_type, recorded_at
                FROM events
                WHERE stream_id = $1
                ORDER BY stream_position DESC
                LIMIT 1
                """,
                loan_stream,
            )
            sub = await conn.fetchrow(
                """
                SELECT payload
                FROM events
                WHERE stream_id = $1 AND event_type = 'ApplicationSubmitted'
                ORDER BY stream_position ASC
                LIMIT 1
                """,
                loan_stream,
            )
            if not sub or not last_ev:
                raise HTTPException(404, "Incomplete application stream")
            p = coerce_jsonb_dict(sub["payload"])
            aid = p.get("applicant_id")
            cn = None
            if aid:
                cn = await conn.fetchval(
                    "SELECT name FROM applicant_registry.companies WHERE company_id = $1",
                    aid,
                )
            base = {
                "application_id": application_id,
                "applicant_id": aid,
                "requested_amount_usd": p.get("requested_amount_usd"),
                "state": None,
                "last_event_type": last_ev["event_type"],
                "last_event_at": last_ev["recorded_at"],
                "company_name": cn or aid,
            }
        # Milestones needed for the decision-path checklist live across multiple streams:
        # loan-{app_id} (submission, triggers, final binding) + docpkg/credit/fraud/compliance (completed verdicts).
        mrows = await conn.fetch(
            """
            SELECT event_type, payload, recorded_at
            FROM (
                SELECT event_type, payload, recorded_at,
                       ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY stream_position DESC) AS rn
                FROM events
                WHERE stream_id = ANY($1::text[])
                  AND event_type = ANY($2::text[])
            ) z
            WHERE rn = 1
            """,
            workflow_streams,
            list(MILESTONE_TYPES),
        )
        stream_rows = await conn.fetch(
            """
            SELECT event_type, stream_position, global_position, recorded_at, event_version
            FROM events
            WHERE stream_id = ANY($1::text[])
            ORDER BY global_position ASC
            """,
            workflow_streams,
        )
        agent_rows = await conn.fetch(
            """
            SELECT payload, recorded_at
            FROM events
            WHERE event_type = 'AgentSessionCompleted'
              AND metadata->>'correlation_id' = $1
            ORDER BY recorded_at DESC
            """,
            application_id,
        )
        by_type = {}
        for mr in mrows:
            by_type[mr["event_type"]] = {
                "payload": coerce_jsonb_dict(mr["payload"]),
                "recorded_at": mr["recorded_at"].isoformat() if mr["recorded_at"] else None,
            }
        await _merge_latest_loan_milestones(conn, loan_stream, by_type)

    milestones = []
    for et in MILESTONE_TYPES:
        if et not in by_type:
            continue
        milestones.append(
            {
                "event_type": et,
                "label": EVENT_LABELS.get(et, et),
                "recorded_at": by_type[et]["recorded_at"],
            }
        )
    milestones.sort(key=lambda x: x["recorded_at"] or "")

    dg = by_type.get("DecisionGenerated", {}).get("payload") or {}
    hr = by_type.get("HumanReviewCompleted", {}).get("payload") or {}

    binding = None
    if "ApplicationApproved" in by_type:
        binding = "approved"
    elif "ApplicationDeclined" in by_type:
        binding = "declined"

    sub_pl = by_type.get("ApplicationSubmitted", {}).get("payload") or {}
    last_at = base.get("last_event_at")
    stream_events = [
        {
            "event_type": r["event_type"],
            "stream_position": int(r["stream_position"]),
            "global_position": int(r["global_position"]),
            "event_version": int(r["event_version"]),
            "recorded_at": _iso_utc(r["recorded_at"]),
            "label": EVENT_LABELS.get(str(r["event_type"]), str(r["event_type"])),
        }
        for r in stream_rows
    ]
    # Surface lightweight performance metrics per agent_type (latest completed session per type).
    agent_timings_ms: dict[str, int] = {}
    for r in agent_rows:
        payload = coerce_jsonb_dict(r["payload"])
        agent_type = str(payload.get("agent_type") or "")
        if not agent_type or agent_type in agent_timings_ms:
            continue
        try:
            agent_timings_ms[agent_type] = int(payload.get("total_duration_ms") or 0)
        except Exception:
            agent_timings_ms[agent_type] = 0

    _hitl_thr, _hitl_req = _hitl_confidence_policy(by_type)
    _auto_rid = (
        os.getenv("LEDGER_AUTO_BINDING_REVIEWER_ID", "SYSTEM-HIGH-CONFIDENCE").strip()
        or "SYSTEM-HIGH-CONFIDENCE"
    )
    _auto_bind_off = os.getenv("LEDGER_AUTO_BIND_HIGH_CONFIDENCE", "1").strip().lower() in (
        "0",
        "false",
        "no",
    )
    return {
        "application_id": application_id,
        "applicant_id": base["applicant_id"],
        "company_name": base.get("company_name") or base["applicant_id"],
        "requested_amount_usd": _fmt_usd(base["requested_amount_usd"]),
        "loan_purpose": _fmt_purpose(sub_pl.get("loan_purpose")),
        "loan_term_months": sub_pl.get("loan_term_months"),
        "submission_channel": sub_pl.get("submission_channel"),
        "submitted_at": by_type.get("ApplicationSubmitted", {}).get("recorded_at"),
        "ui_stage": _ui_stage(base.get("last_event_type")),
        "summary_projection_state": base.get("state"),
        "last_event_type": base.get("last_event_type"),
        "last_event_at": _iso_utc(last_at) if isinstance(last_at, datetime) else last_at,
        "binding_recorded_at": by_type.get("ApplicationApproved", {}).get("recorded_at")
        or by_type.get("ApplicationDeclined", {}).get("recorded_at"),
        "stream_events": stream_events,
        "decision_path": _decision_path(by_type),
        "orchestrator_recommendation": {
            "recommendation": dg.get("recommendation"),
            "confidence": dg.get("confidence_score") or dg.get("confidence"),
            "summary": dg.get("executive_summary"),
            "at": by_type.get("DecisionGenerated", {}).get("recorded_at"),
        }
        if dg
        else None,
        "human_review": {
            "reviewer_id": hr.get("reviewer_id"),
            "original_recommendation": hr.get("original_recommendation"),
            "final_decision": hr.get("final_decision"),
            "override": hr.get("override"),
            "decision_reason": hr.get("override_reason"),
            "at": by_type.get("HumanReviewCompleted", {}).get("recorded_at"),
            "automated": str(hr.get("reviewer_id") or "") == _auto_rid,
        }
        if hr
        else None,
        "binding_outcome": binding,
        "milestones": milestones,
        "workflow_metrics": {
            "agent_timings_ms": agent_timings_ms,
        },
        "hitl_confidence_threshold": _hitl_thr,
        "hitl_required_by_policy": _hitl_req,
        "human_review_requested": "HumanReviewRequested" in by_type,
        "staff_binding_required": _hitl_req or _auto_bind_off,
    }


class SubmitApplicationBody(BaseModel):
    application_id: str
    applicant_id: str
    requested_amount_usd: Decimal = Field(gt=Decimal("0"))
    loan_purpose: str | None = None
    loan_term_months: int | None = None
    submission_channel: str | None = None
    contact_email: str | None = None
    contact_name: str | None = None


class HumanReviewCompleteBody(BaseModel):
    reviewer_id: str
    override: bool
    original_recommendation: str
    final_decision: str
    override_reason: str | None = None
    decision_reason: str | None = None


@app.post("/api/applications")
async def post_application(body: SubmitApplicationBody) -> dict[str, Any]:
    if _store is None:
        raise HTTPException(503, "Store unavailable")
    pool = _pool()
    async with pool.acquire() as conn:
        ok = await conn.fetchval(
            "SELECT 1 FROM applicant_registry.companies WHERE company_id = $1",
            body.applicant_id,
        )
    if not ok:
        raise HTTPException(
            400,
            f"applicant_id {body.applicant_id!r} not found in applicant_registry — onboard the company in CRM first.",
        )
    if body.loan_purpose:
        try:
            LoanPurpose(body.loan_purpose)
        except ValueError as e:
            raise HTTPException(400, f"invalid loan_purpose: {body.loan_purpose!r}") from e

    cmd = SubmitApplicationCommand(
        application_id=body.application_id,
        applicant_id=body.applicant_id,
        requested_amount_usd=body.requested_amount_usd,
        loan_purpose=body.loan_purpose,
        loan_term_months=body.loan_term_months,
        submission_channel=body.submission_channel,
        contact_email=body.contact_email,
        contact_name=body.contact_name,
        application_reference=body.application_id,
    )
    try:
        await handle_submit_application(cmd, _store)
    except DomainError as e:
        raise HTTPException(409, e.message) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e

    version = await _store.stream_version(f"loan-{body.application_id}")
    return {"ok": True, "stream_id": f"loan-{body.application_id}", "current_version": version}


def _llm_client():
    # OpenRouter-only mode:
    # BaseApexAgent routes all LLM calls directly via OPENROUTER env vars.
    # So we intentionally pass `None` here.
    return None


def _document_format_from_filename(filename: str) -> DocumentFormat:
    ext = Path(filename).suffix.lower().lstrip(".")
    if ext == "pdf":
        return DocumentFormat.PDF
    if ext in {"xlsx", "xlsm"}:
        return DocumentFormat.XLSX
    if ext == "csv":
        return DocumentFormat.CSV
    # Default to PDF for unknown types; downstream validation will fail loudly.
    return DocumentFormat.PDF


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


async def _append_document_uploaded_events(
    *,
    store: EventStore,
    application_id: str,
    uploader: str,
    files: dict[str, UploadFile],
) -> dict[str, Any]:
    stream_id = f"loan-{application_id}"

    # Avoid duplicate append: if the same document_id already exists, skip it.
    existing_events = await store.load_stream(stream_id)
    existing_doc_ids = {
        (e.get("payload") or {}).get("document_id")
        for e in existing_events
        if isinstance(e, dict) and e.get("event_type") == "DocumentUploaded"
    }

    base_dir = Path(os.getenv("DOCUMENT_UPLOAD_DIR", "./data/uploads")).resolve()
    target_dir = base_dir / application_id
    target_dir.mkdir(parents=True, exist_ok=True)

    to_append: list[dict[str, Any]] = []
    uploaded: list[str] = []
    skipped: list[str] = []

    # Map upload field -> required document type.
    mapping = {
        "proposal": DocumentType.APPLICATION_PROPOSAL,
        "income": DocumentType.INCOME_STATEMENT,
        "balance": DocumentType.BALANCE_SHEET,
    }

    for field, up in files.items():
        if field not in mapping:
            continue
        document_type = mapping[field]

        content = await up.read()
        doc_hash = _sha256_bytes(content)
        document_id = doc_hash[:12]
        if document_id in existing_doc_ids:
            skipped.append(document_id)
            continue

        fmt = _document_format_from_filename(up.filename or f"{document_type.value}.pdf")
        ext = Path(up.filename or "").suffix.lower()
        if not ext:
            ext = f".{fmt.value}"

        target = target_dir / f"{document_id}{ext}"
        target.write_bytes(content)

        st_size = len(content)
        to_append.append(
            DocumentUploaded(
                application_id=application_id,
                document_id=document_id,
                document_type=document_type,
                document_format=fmt,
                filename=Path(up.filename or "").name or target.name,
                file_path=str(target),
                file_size_bytes=st_size,
                file_hash=doc_hash,
                fiscal_year=2024,
                uploaded_at=datetime.now(UTC),
                uploaded_by=uploader,
            ).to_store_dict()
        )
        uploaded.append(document_id)

    if to_append:
        expected = await store.stream_version(stream_id)
        await store.append(
            stream_id=stream_id,
            events=to_append,
            expected_version=expected,
            correlation_id=application_id,
            causation_id="document_upload_portal",
        )

    return {"ok": True, "uploaded": uploaded, "skipped": skipped, "app_id": application_id}


async def _run_workflow(application_id: str) -> dict[str, Any]:
    global _store
    if _store is None:
        return {"ok": False, "error": "Store unavailable"}

    # Enable agent/node-level terminal logging for this workflow run.
    if not os.getenv("LEDGER_AGENT_VERBOSE"):
        os.environ["LEDGER_AGENT_VERBOSE"] = "1"

    from agents.document_processing_agent import DocumentProcessingAgent
    from agents.credit_analysis_agent import CreditAnalysisAgent
    from agents.stub_agents import ComplianceAgent, FraudDetectionAgent, DecisionOrchestratorAgent

    print(f"[workflow] start application_id={application_id}", flush=True)
    client = _llm_client()
    registry = None  # ApplicantRegistryClient is stubbed in this repo; agents degrade gracefully.

    def _env_int(name: str, default: int) -> int:
        raw = os.getenv(name, "").strip()
        if not raw:
            return int(default)
        try:
            return int(raw)
        except Exception:
            return int(default)

    async def _append_workflow_error(*, stage: str, code: str, message: str) -> None:
        if _store is None:
            return
        evt = {
            "event_type": "DomainError",
            "event_version": 1,
            "payload": {
                "application_id": application_id,
                "session_id": f"workflow-{application_id}",
                "node_name": stage,
                "document_id": None,
                "code": code,
                "message": message[:500],
                "details": {"stage": stage},
                "recorded_at": datetime.now(UTC).isoformat(),
            },
        }
        try:
            ver = await _store.stream_version(f"loan-{application_id}")
            await _store.append(
                stream_id=f"loan-{application_id}",
                events=[evt],
                expected_version=ver,
                correlation_id=application_id,
                causation_id=f"workflow-{application_id}",
            )
        except Exception:
            # Never let audit append failures block the workflow.
            return

    def _event_type(e: Any) -> str | None:
        if isinstance(e, dict):
            v = e.get("event_type")
            return str(v) if v else None
        v = getattr(e, "event_type", None)
        return str(v) if v else None

    try:
        loan_stream = f"loan-{application_id}"
        before = await _store.load_stream(loan_stream)
        before_types = {_event_type(e) for e in before}
        before_types.discard(None)

        doc_agent = DocumentProcessingAgent(
            "doc-agent-portal", "document_processing", _store, registry, client
        )
        doc_timeout = float(_env_int("WORKFLOW_DOC_TIMEOUT_S", 25))
        try:
            await asyncio.wait_for(doc_agent.process_application(application_id), timeout=doc_timeout)
        except asyncio.TimeoutError:
            await _append_workflow_error(
                stage="document_processing",
                code="workflow_timeout",
                message=f"document_processing exceeded {doc_timeout:.0f}s",
            )
            return {"ok": False, "application_id": application_id, "error": "document_processing_timeout"}
        print(f"[workflow] document_processing completed application_id={application_id}", flush=True)

        credit_agent = CreditAnalysisAgent("credit-agent-portal", "credit_analysis", _store, registry, client)
        credit_timeout = float(_env_int("WORKFLOW_CREDIT_TIMEOUT_S", 15))
        try:
            await asyncio.wait_for(credit_agent.process_application(application_id), timeout=credit_timeout)
        except asyncio.TimeoutError:
            await _append_workflow_error(
                stage="credit_analysis",
                code="workflow_timeout",
                message=f"credit_analysis exceeded {credit_timeout:.0f}s",
            )
            return {"ok": False, "application_id": application_id, "error": "credit_analysis_timeout"}
        print(f"[workflow] credit_analysis completed application_id={application_id}", flush=True)

        fraud_agent = FraudDetectionAgent("fraud-agent-portal", "fraud_detection", _store, registry, client)
        fraud_timeout = float(_env_int("WORKFLOW_FRAUD_TIMEOUT_S", 12))
        try:
            await asyncio.wait_for(fraud_agent.process_application(application_id), timeout=fraud_timeout)
        except asyncio.TimeoutError:
            await _append_workflow_error(
                stage="fraud_detection",
                code="workflow_timeout",
                message=f"fraud_detection exceeded {fraud_timeout:.0f}s",
            )
            return {"ok": False, "application_id": application_id, "error": "fraud_detection_timeout"}
        print(f"[workflow] fraud_detection completed application_id={application_id}", flush=True)

        compliance_agent = ComplianceAgent("compliance-agent-portal", "compliance", _store, registry, client)
        compliance_timeout = float(_env_int("WORKFLOW_COMPLIANCE_TIMEOUT_S", 12))
        try:
            await asyncio.wait_for(compliance_agent.process_application(application_id), timeout=compliance_timeout)
        except asyncio.TimeoutError:
            await _append_workflow_error(
                stage="compliance",
                code="workflow_timeout",
                message=f"compliance exceeded {compliance_timeout:.0f}s",
            )
            return {"ok": False, "application_id": application_id, "error": "compliance_timeout"}
        print(f"[workflow] compliance completed application_id={application_id}", flush=True)

        loan_events = await _store.load_stream(loan_stream)
        types = {_event_type(e) for e in loan_events}
        types.discard(None)
        # Only run orchestrator if compliance didn’t hard-block and we haven't
        # already produced the non-binding recommendation.
        #
        # Since binding is now emitted after explicit human action, we must gate on
        # `DecisionGenerated` rather than waiting for `ApplicationApproved/Declined`.
        if (
            "DecisionRequested" in types
            and "DecisionGenerated" not in types
            and not ({"ApplicationApproved", "ApplicationDeclined"} & types)
        ):
            orch_enabled_flag = os.getenv("WORKFLOW_ORCH_LLM_ENABLED", "1").strip().lower() in ("1", "true", "yes", "y")
            raw_key = os.getenv("OPENROUTER_API_KEY", "").strip()
            has_openrouter = bool(raw_key) and "YOUR_KEY_HERE" not in raw_key
            use_llm = orch_enabled_flag and has_openrouter

            orch_timeout = float(_env_int("WORKFLOW_ORCH_TIMEOUT_S", 20))
            if use_llm:
                os.environ.pop("LEDGER_DETERMINISTIC_ONLY", None)
            else:
                os.environ["LEDGER_DETERMINISTIC_ONLY"] = "1"
                if orch_enabled_flag and not has_openrouter:
                    await _append_workflow_error(
                        stage="decision_orchestrator",
                        code="orchestrator_deterministic_fallback",
                        message=(
                            "OPENROUTER_API_KEY unset; orchestrator LLM skipped — "
                            "deterministic synthesis will still emit DecisionGenerated."
                        ),
                    )

            orch = DecisionOrchestratorAgent(
                "orchestrator-agent-portal",
                "decision_orchestrator",
                _store,
                registry,
                client,
            )
            try:
                await asyncio.wait_for(orch.process_application(application_id), timeout=orch_timeout)
            except asyncio.TimeoutError:
                await _append_workflow_error(
                    stage="decision_orchestrator",
                    code="workflow_timeout",
                    message=f"orchestrator exceeded {orch_timeout:.0f}s",
                )
            except Exception as e:
                await _append_workflow_error(
                    stage="decision_orchestrator",
                    code="workflow_error",
                    message=f"orchestrator failed: {type(e).__name__}: {e}",
                )

        after = await _store.load_stream(loan_stream)
        after_types = {_event_type(e) for e in after}
        after_types.discard(None)
        added = sorted(after_types - before_types)

        return {"ok": True, "application_id": application_id, "loan_events_added": added}
    except asyncio.CancelledError:
        print(f"[workflow] cancelled application_id={application_id}", flush=True)
        raise
    except Exception as e:
        print(f"[workflow] ERROR application_id={application_id} err={type(e).__name__}: {e}", flush=True)
        await _append_workflow_error(
            stage="workflow",
            code="workflow_error",
            message=f"{type(e).__name__}: {e}",
        )
        return {"ok": False, "application_id": application_id, "error": type(e).__name__}


@app.post("/api/applications/{application_id}/documents")
async def upload_application_documents(
    application_id: str,
    proposal: UploadFile = File(...),
    income: UploadFile = File(...),
    balance: UploadFile = File(...),
    auto_run: bool = Query(True, description="Run document->credit->fraud->compliance workflow after upload"),
) -> dict[str, Any]:
    if _store is None:
        raise HTTPException(503, "Store unavailable")

    res = await _append_document_uploaded_events(
        store=_store,
        application_id=application_id,
        uploader="portal",
        files={"proposal": proposal, "income": income, "balance": balance},
    )

    if auto_run and res.get("ok"):
        _start_workflow_task(application_id)

    return res


@app.post("/api/applications/{application_id}/run-workflow")
async def run_workflow_endpoint(application_id: str) -> dict[str, Any]:
    if _store is None:
        raise HTTPException(503, "Store unavailable")
    _start_workflow_task(application_id)
    return {"ok": True, "application_id": application_id, "started": True}


@app.post("/api/applications/{application_id}/human-review")
async def post_human_review(application_id: str, body: HumanReviewCompleteBody) -> dict[str, Any]:
    if _store is None:
        raise HTTPException(503, "Store unavailable")

    rationale = (body.decision_reason or body.override_reason or "").strip()
    if not rationale:
        raise HTTPException(422, "decision_reason is required (brief rationale for approve or decline).")

    svc = LedgerMCPService(_store)
    out = await svc.record_human_review(
        {
            "application_id": application_id,
            "reviewer_id": body.reviewer_id,
            "override": body.override,
            "original_recommendation": body.original_recommendation,
            "final_decision": body.final_decision,
            "override_reason": rationale,
            "decision_reason": rationale,
        },
        CallerContext(role="human_reviewer"),
    )
    if not out.get("ok"):
        raise HTTPException(400, out.get("message") or str(out))
    return out
