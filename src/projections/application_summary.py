from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import asyncpg

from event_store import EventStore, coerce_jsonb_dict
from models.events import DecisionRecommendation, StoredEvent

from projections.base import Projection
from projections.util import _app_id, _event_time

logger = logging.getLogger("projections")

class ApplicationSummaryProjection(Projection):
    name = "application_summary"
    subscribed_event_types = {
        "ApplicationSubmitted",
        "CreditAnalysisCompleted",
        "FraudScreeningCompleted",
        "ComplianceCheckCompleted",
        "DecisionGenerated",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
        "AgentSessionCompleted",
    }

    def __init__(self, store: EventStore, *, snapshot_every_events: int = 50) -> None:
        super().__init__(store)
        self._snapshot_every_events = max(1, int(snapshot_every_events))

    async def _ensure_row(self, conn: asyncpg.Connection, app_id: str) -> None:
        await conn.execute(
            """
            INSERT INTO application_summary(application_id)
            VALUES ($1)
            ON CONFLICT (application_id) DO NOTHING
            """,
            app_id,
        )

    async def _handle_with_conn(self, conn: asyncpg.Connection, event: StoredEvent) -> None:
        app_id = _app_id(event)
        if not app_id:
            return
        await self._ensure_row(conn, app_id)
        payload = event.payload
        event_at = _event_time(event)
        if event.event_type == "ApplicationSubmitted":
            await conn.execute(
                    """
                    UPDATE application_summary
                    SET state = 'SUBMITTED',
                        applicant_id = $2,
                        requested_amount_usd = $3,
                        last_event_type = $4,
                        last_event_at = $5
                    WHERE application_id = $1
                    """,
                    app_id,
                    payload.get("applicant_id"),
                    payload.get("requested_amount_usd"),
                    event.event_type,
                    event_at,
                )
        elif event.event_type == "CreditAnalysisCompleted":
            await conn.execute(
                    """
                    UPDATE application_summary
                    SET state = 'CREDIT_ANALYSIS_COMPLETE',
                        risk_tier = $2,
                        approved_amount_usd = COALESCE($3::numeric, approved_amount_usd),
                        last_event_type = $4,
                        last_event_at = $5
                    WHERE application_id = $1
                    """,
                    app_id,
                    payload.get("risk_tier"),
                    payload.get("recommended_limit_usd"),
                    event.event_type,
                    event_at,
                )
        elif event.event_type == "FraudScreeningCompleted":
            await conn.execute(
                    """
                    UPDATE application_summary
                    SET fraud_score = $2,
                        state = 'FRAUD_SCREENING_COMPLETE',
                        last_event_type = $3,
                        last_event_at = $4
                    WHERE application_id = $1
                    """,
                    app_id,
                    payload.get("fraud_score"),
                    event.event_type,
                    event_at,
                )
        elif event.event_type == "ComplianceCheckCompleted":
            await conn.execute(
                    """
                    UPDATE application_summary
                    SET compliance_status = $2,
                        state = 'COMPLIANCE_CHECK_COMPLETE',
                        last_event_type = $3,
                        last_event_at = $4
                    WHERE application_id = $1
                    """,
                    app_id,
                    payload.get("overall_verdict"),
                    event.event_type,
                    event_at,
                )
        elif event.event_type == "DecisionGenerated":
            await conn.execute(
                    """
                    UPDATE application_summary
                    SET decision = $2,
                        state = CASE
                            WHEN $2 = 'APPROVE' THEN 'APPROVED'
                            WHEN $2 = 'DECLINE' THEN 'DECLINED'
                            ELSE 'PENDING_HUMAN_REVIEW'
                        END,
                        agent_sessions_completed = COALESCE($3::jsonb, '[]'::jsonb),
                        last_event_type = $4,
                        last_event_at = $5
                    WHERE application_id = $1
                    """,
                    app_id,
                    payload.get("recommendation"),
                    json.dumps(payload.get("contributing_agent_sessions", [])),
                    event.event_type,
                    event_at,
                )
        elif event.event_type == "HumanReviewCompleted":
            await conn.execute(
                    """
                    UPDATE application_summary
                    SET human_reviewer_id = $2,
                        decision = $3,
                        final_decision_at = $4,
                        last_event_type = $5,
                        last_event_at = $6
                    WHERE application_id = $1
                    """,
                    app_id,
                    payload.get("reviewer_id"),
                    payload.get("final_decision"),
                    payload.get("reviewed_at"),
                    event.event_type,
                    event_at,
                )
        elif event.event_type == "ApplicationApproved":
            await conn.execute(
                    """
                    UPDATE application_summary
                    SET state = 'APPROVED',
                        approved_amount_usd = $2,
                        decision = 'APPROVE',
                        final_decision_at = $3,
                        last_event_type = $4,
                        last_event_at = $5
                    WHERE application_id = $1
                    """,
                    app_id,
                    payload.get("approved_amount_usd"),
                    payload.get("approved_at"),
                    event.event_type,
                    event_at,
                )
        elif event.event_type == "ApplicationDeclined":
            await conn.execute(
                    """
                    UPDATE application_summary
                    SET state = 'DECLINED',
                        decision = 'DECLINE',
                        final_decision_at = $2,
                        last_event_type = $3,
                        last_event_at = $4
                    WHERE application_id = $1
                    """,
                    app_id,
                    payload.get("declined_at"),
                    event.event_type,
                    event_at,
                )
        elif event.event_type == "AgentSessionCompleted":
            sid = payload.get("session_id")
            if sid:
                await conn.execute(
                        """
                        UPDATE application_summary
                        SET agent_sessions_completed = (
                            SELECT to_jsonb(array_agg(DISTINCT elem))
                            FROM (
                                SELECT jsonb_array_elements_text(COALESCE(agent_sessions_completed, '[]'::jsonb)) elem
                                UNION ALL
                                SELECT $2
                            ) s
                        ),
                        last_event_type = $3,
                        last_event_at = $4
                        WHERE application_id = $1
                        """,
                        app_id,
                        sid,
                        event.event_type,
                        event_at,
                    )

        # Optional snapshots: persist a compact read-model state for replay/time-travel diagnostics.
        # This is intentionally fast, deterministic, and bounded (periodic).
        enabled = os.getenv("LEDGER_SNAPSHOTS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "y")
        if not enabled:
            return
        if int(event.stream_position) % self._snapshot_every_events != 0:
            return

        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            app_id,
        )
        if row is None:
            return
        state = dict(row)
        # Ensure datetimes are JSON-serializable.
        for k, v in list(state.items()):
            if isinstance(v, datetime):
                state[k] = v.isoformat()

        aggregate_type = str(event.stream_id).split("-", 1)[0]
        await conn.execute(
            """
            INSERT INTO snapshots(stream_id, stream_position, aggregate_type, snapshot_version, state)
            VALUES($1, $2, $3, $4, $5::jsonb)
            """,
            event.stream_id,
            int(event.stream_position),
            aggregate_type,
            1,
            json.dumps(state),
        )

    async def _reset_state(self) -> None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM snapshots WHERE aggregate_type = 'loan'")
            await conn.execute("TRUNCATE TABLE application_summary")


