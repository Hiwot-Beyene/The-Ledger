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

class AgentPerformanceLedgerProjection(Projection):
    name = "agent_performance_ledger"
    subscribed_event_types = {
        "AgentSessionStarted",
        "CreditAnalysisCompleted",
        "DecisionGenerated",
        "HumanReviewCompleted",
    }

    async def _upsert_metric(
        self,
        conn: asyncpg.Connection,
        *,
        agent_id: str,
        model_version: str,
        observed_at: datetime,
        analysis_inc: int = 0,
        decision_inc: int = 0,
        conf: float | None = None,
        duration_ms: int | None = None,
        approve_inc: int = 0,
        decline_inc: int = 0,
        refer_inc: int = 0,
        override_inc: int = 0,
    ) -> None:
        await conn.execute(
                """
                INSERT INTO agent_performance_ledger(
                    agent_id, model_version, analyses_completed, decisions_generated,
                    avg_confidence_score, avg_duration_ms,
                    approve_rate, decline_rate, refer_rate, human_override_rate,
                    first_seen_at, last_seen_at
                )
                VALUES($1, $2, $3, $4, COALESCE($5, 0), COALESCE($6, 0), 0, 0, 0, 0, $7, $7)
                ON CONFLICT (agent_id, model_version)
                DO UPDATE SET
                    analyses_completed = agent_performance_ledger.analyses_completed + $3,
                    decisions_generated = agent_performance_ledger.decisions_generated + $4,
                    avg_confidence_score = CASE
                        WHEN $5 IS NULL THEN agent_performance_ledger.avg_confidence_score
                        WHEN (agent_performance_ledger.analyses_completed + $3) = 0 THEN $5
                        ELSE (
                            (
                                agent_performance_ledger.avg_confidence_score
                                * agent_performance_ledger.analyses_completed
                            ) + ($5 * $3)
                        ) / NULLIF(agent_performance_ledger.analyses_completed + $3, 0)
                    END,
                    avg_duration_ms = CASE
                        WHEN $6 IS NULL THEN agent_performance_ledger.avg_duration_ms
                        WHEN (agent_performance_ledger.analyses_completed + $3) = 0 THEN $6
                        ELSE (
                            (
                                agent_performance_ledger.avg_duration_ms
                                * agent_performance_ledger.analyses_completed
                            ) + ($6 * $3)
                        ) / NULLIF(agent_performance_ledger.analyses_completed + $3, 0)
                    END,
                    approve_rate = CASE
                        WHEN (agent_performance_ledger.decisions_generated + $4) = 0
                        THEN agent_performance_ledger.approve_rate
                        ELSE (
                            (
                                agent_performance_ledger.approve_rate
                                * agent_performance_ledger.decisions_generated
                            ) + $8
                        ) / NULLIF(agent_performance_ledger.decisions_generated + $4, 0)
                    END,
                    decline_rate = CASE
                        WHEN (agent_performance_ledger.decisions_generated + $4) = 0
                        THEN agent_performance_ledger.decline_rate
                        ELSE (
                            (
                                agent_performance_ledger.decline_rate
                                * agent_performance_ledger.decisions_generated
                            ) + $9
                        ) / NULLIF(agent_performance_ledger.decisions_generated + $4, 0)
                    END,
                    refer_rate = CASE
                        WHEN (agent_performance_ledger.decisions_generated + $4) = 0
                        THEN agent_performance_ledger.refer_rate
                        ELSE (
                            (
                                agent_performance_ledger.refer_rate
                                * agent_performance_ledger.decisions_generated
                            ) + $10
                        ) / NULLIF(agent_performance_ledger.decisions_generated + $4, 0)
                    END,
                    human_override_rate = CASE
                        WHEN (agent_performance_ledger.decisions_generated + $4) = 0
                        THEN agent_performance_ledger.human_override_rate
                        ELSE (
                            (
                                agent_performance_ledger.human_override_rate
                                * agent_performance_ledger.decisions_generated
                            ) + $11
                        ) / NULLIF(agent_performance_ledger.decisions_generated + $4, 0)
                    END,
                    last_seen_at = GREATEST(agent_performance_ledger.last_seen_at, $7)
                """,
                agent_id,
                model_version,
                analysis_inc,
                decision_inc,
                conf,
                duration_ms,
                observed_at,
                approve_inc,
                decline_inc,
                refer_inc,
                override_inc,
            )

    async def _handle_with_conn(self, conn: asyncpg.Connection, event: StoredEvent) -> None:
        payload = event.payload
        event_at = _event_time(event)
        if event.event_type == "AgentSessionStarted":
            await conn.execute(
                    """
                    INSERT INTO agent_session_index(session_id, application_id, agent_id, model_version)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (session_id)
                    DO UPDATE SET
                        application_id = EXCLUDED.application_id,
                        agent_id = EXCLUDED.agent_id,
                        model_version = EXCLUDED.model_version
                    """,
                    payload.get("session_id"),
                    payload.get("application_id"),
                    payload.get("agent_id"),
                    payload.get("model_version"),
                )
            return

        if event.event_type == "CreditAnalysisCompleted":
            agent_id = str(payload.get("agent_id", "")).strip()
            model_version = str(payload.get("model_version", "")).strip()
            if agent_id and model_version:
                await self._upsert_metric(
                        conn,
                        agent_id=agent_id,
                        model_version=model_version,
                        observed_at=event_at,
                        analysis_inc=1,
                        conf=float(payload.get("confidence_score", 0.0)),
                        duration_ms=int(payload.get("analysis_duration_ms", 0)),
                    )
            return

        if event.event_type == "DecisionGenerated":
            recommendation = str(payload.get("recommendation", "")).upper()
            approve_inc = 1 if recommendation == DecisionRecommendation.APPROVE.value else 0
            decline_inc = 1 if recommendation == DecisionRecommendation.DECLINE.value else 0
            refer_inc = 1 if recommendation == DecisionRecommendation.REFER.value else 0
            model_versions = payload.get("model_versions", {}) or {}
            orchestrator_model = str(model_versions.get("decision_orchestrator", "")).strip()
            if orchestrator_model:
                await self._upsert_metric(
                        conn,
                        agent_id="decision_orchestrator",
                        model_version=orchestrator_model,
                        observed_at=event_at,
                        decision_inc=1,
                        approve_inc=approve_inc,
                        decline_inc=decline_inc,
                        refer_inc=refer_inc,
                    )
            app_id = _app_id(event)
            if app_id and orchestrator_model:
                await conn.execute(
                        """
                        INSERT INTO application_decision_attribution(application_id, model_version)
                        VALUES ($1, $2)
                        ON CONFLICT (application_id) DO UPDATE SET model_version = EXCLUDED.model_version
                        """,
                        app_id,
                        orchestrator_model,
                    )
            return

        if event.event_type == "HumanReviewCompleted" and payload.get("override") is True:
            app_id = _app_id(event)
            if not app_id:
                return
            row = await conn.fetchrow(
                    """
                    SELECT model_version
                    FROM application_decision_attribution
                    WHERE application_id = $1
                    """,
                    app_id,
                )
            if row and row["model_version"]:
                await self._upsert_metric(
                        conn,
                        agent_id="decision_orchestrator",
                        model_version=str(row["model_version"]),
                        observed_at=event_at,
                        decision_inc=1,
                        override_inc=1,
                    )

    async def _reset_state(self) -> None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                TRUNCATE TABLE
                    agent_performance_ledger,
                    agent_session_index,
                    application_decision_attribution
                """
            )


