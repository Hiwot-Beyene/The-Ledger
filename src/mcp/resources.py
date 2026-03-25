"""MCP projection-backed resources and query port (``ledger://`` read side)."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

from event_store import EventStore, InMemoryEventStore
from projections import ComplianceAuditViewProjection
from what_if.projector import ApplicationSummaryReplayProjection, ComplianceAuditReplayProjection

class ProjectionQueryPort(Protocol):
    async def get_application_summary(self, application_id: str) -> dict[str, Any] | None: ...
    async def get_compliance_current(self, application_id: str) -> dict[str, Any] | None: ...
    async def get_compliance_as_of(self, application_id: str, as_of: datetime) -> dict[str, Any] | None: ...
    async def get_agent_performance(self, agent_id: str) -> list[dict[str, Any]]: ...
    async def get_projection_lags(self) -> dict[str, int]: ...
    async def get_application_audit_read_model(self, application_id: str) -> dict[str, Any] | None: ...
    async def get_agent_session_read_model(self, agent_id: str, session_id: str) -> dict[str, Any] | None: ...


class SqlProjectionQueries:
    def __init__(self, store: EventStore) -> None:
        self._store = store

    async def get_application_summary(self, application_id: str) -> dict[str, Any] | None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM application_summary WHERE application_id = $1",
                application_id,
            )
            return dict(row) if row else None

    async def get_compliance_current(self, application_id: str) -> dict[str, Any] | None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM compliance_audit_current WHERE application_id = $1",
                application_id,
            )
            return dict(row) if row else None

    async def get_compliance_as_of(self, application_id: str, as_of: datetime) -> dict[str, Any] | None:
        from projections import ComplianceAuditViewProjection

        return await ComplianceAuditViewProjection(self._store).get_compliance_at(application_id, as_of)

    async def get_agent_performance(self, agent_id: str) -> list[dict[str, Any]]:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM agent_performance_ledger WHERE agent_id = $1 ORDER BY model_version",
                agent_id,
            )
            return [dict(r) for r in rows]

    async def get_projection_lags(self) -> dict[str, int]:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT projection_name, last_position FROM projection_checkpoints")
            latest = int(await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events"))
            return {str(r["projection_name"]): max(0, latest - int(r["last_position"])) for r in rows}

    async def get_application_audit_read_model(self, application_id: str) -> dict[str, Any] | None:
        summary = await self.get_application_summary(application_id)
        if not summary:
            return None
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            sessions = await conn.fetch(
                """
                SELECT session_id, application_id, agent_id, model_version
                FROM agent_session_index
                WHERE application_id = $1
                ORDER BY session_id
                """,
                application_id,
            )
            attr = await conn.fetchrow(
                """
                SELECT application_id, model_version
                FROM application_decision_attribution
                WHERE application_id = $1
                """,
                application_id,
            )
        return {
            "application_id": application_id,
            "application_summary": dict(summary),
            "indexed_agent_sessions": [dict(r) for r in sessions],
            "decision_attribution": dict(attr) if attr else None,
            "source": "sql_read_models",
        }

    async def get_agent_session_read_model(self, agent_id: str, session_id: str) -> dict[str, Any] | None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT session_id, application_id, agent_id, model_version
                FROM agent_session_index
                WHERE session_id = $1 AND agent_id = $2
                """,
                session_id,
                agent_id,
            )
            return dict(row) if row else None


class InMemoryProjectionQueries:
    """Read-model emulation for ``InMemoryEventStore`` (used by MCP resources, not ``LedgerMCPService.resource_*``)."""

    def __init__(self, store: InMemoryEventStore) -> None:
        self._store = store

    async def get_application_summary(self, application_id: str) -> dict[str, Any] | None:
        events = await self._store.load_stream(f"loan-{application_id}")
        if not events:
            return None
        proj = ApplicationSummaryReplayProjection()
        for ev in events:
            proj.apply(ev)
        snap = proj.snapshot()
        snap.setdefault("application_id", application_id)
        return snap

    async def get_compliance_current(self, application_id: str) -> dict[str, Any] | None:
        events = await self._store.load_stream(f"loan-{application_id}")
        proj = ComplianceAuditReplayProjection()
        for ev in events:
            proj.apply(ev)
        row = proj.snapshot()
        if not row.get("overall_verdict") and int(row.get("rules_evaluated") or 0) <= 0:
            return None
        return {
            "application_id": application_id,
            "verdict": row.get("overall_verdict"),
            "rules_evaluated": row.get("rules_evaluated"),
            "rules_passed": row.get("rules_passed"),
            "rules_failed": row.get("rules_failed"),
            "rules_noted": row.get("rules_noted"),
            "has_hard_block": row.get("has_hard_block"),
            "latest_event_at": row.get("updated_at"),
        }

    async def get_compliance_as_of(self, application_id: str, as_of: datetime) -> dict[str, Any] | None:
        boundary = as_of.astimezone(UTC)
        events = await self._store.load_stream(f"loan-{application_id}")
        proj = ComplianceAuditReplayProjection()
        for ev in events:
            if ev.recorded_at.astimezone(UTC) <= boundary:
                proj.apply(ev)
        row = proj.snapshot()
        if not row.get("overall_verdict") and int(row.get("rules_evaluated") or 0) <= 0:
            return None
        return {
            "application_id": application_id,
            "verdict": row.get("overall_verdict"),
            "rules_evaluated": row.get("rules_evaluated"),
            "rules_passed": row.get("rules_passed"),
            "rules_failed": row.get("rules_failed"),
            "rules_noted": row.get("rules_noted"),
            "has_hard_block": row.get("has_hard_block"),
            "latest_event_at": row.get("updated_at"),
            "as_of": boundary.isoformat(),
        }

    async def get_agent_performance(self, agent_id: str) -> list[dict[str, Any]]:
        return []

    async def get_projection_lags(self) -> dict[str, int]:
        return {}

    async def get_application_audit_read_model(self, application_id: str) -> dict[str, Any] | None:
        summary = await self.get_application_summary(application_id)
        if not summary:
            return None
        milestones: list[dict[str, Any]] = []
        sessions: list[dict[str, Any]] = []
        for ev in await self._store.load_stream(f"loan-{application_id}"):
            milestones.append(
                {
                    "stream_id": ev.stream_id,
                    "stream_position": ev.stream_position,
                    "global_position": ev.global_position,
                    "event_type": ev.event_type,
                    "event_id": str(ev.event_id),
                    "recorded_at": ev.recorded_at.astimezone(UTC).isoformat(),
                }
            )
        async for ev in self._store.load_all(from_global_position=0, batch_size=500):
            if ev.event_type != "AgentSessionStarted":
                continue
            pl = ev.payload
            if str(pl.get("application_id")) != application_id:
                continue
            sessions.append(
                {
                    "session_id": pl.get("session_id"),
                    "application_id": application_id,
                    "agent_id": pl.get("agent_id"),
                    "model_version": pl.get("model_version"),
                }
            )
        return {
            "application_id": application_id,
            "application_summary": summary,
            "milestones": milestones,
            "indexed_agent_sessions": sessions,
            "decision_attribution": None,
            "source": "in_memory_replay",
        }

    async def get_agent_session_read_model(self, agent_id: str, session_id: str) -> dict[str, Any] | None:
        events = await self._store.load_stream(f"agent-{agent_id}-{session_id}")
        for ev in events:
            if ev.event_type == "AgentSessionStarted":
                pl = ev.payload
                return {
                    "session_id": session_id,
                    "application_id": pl.get("application_id"),
                    "agent_id": pl.get("agent_id"),
                    "model_version": pl.get("model_version"),
                }
        return None


