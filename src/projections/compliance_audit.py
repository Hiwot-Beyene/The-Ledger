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

_COMPLIANCE_TABLE_PAIRS: frozenset = frozenset(
    {
        ("compliance_audit_current", "compliance_audit_snapshots"),
        ("compliance_audit_current_bg", "compliance_audit_snapshots_bg"),
    }
)


class ComplianceAuditViewProjection(Projection):
    name = "compliance_audit_view"
    subscribed_event_types = {
        "ComplianceCheckInitiated",
        "ComplianceRulePassed",
        "ComplianceRuleFailed",
        "ComplianceRuleNoted",
        "ComplianceCheckCompleted",
    }

    def __init__(
        self,
        store: EventStore,
        *,
        snapshot_every_events: int = 25,
        current_table: str = "compliance_audit_current",
        snapshots_table: str = "compliance_audit_snapshots",
        checkpoint_name: str | None = None,
    ) -> None:
        super().__init__(store)
        pair = (current_table, snapshots_table)
        if pair not in _COMPLIANCE_TABLE_PAIRS:
            raise ValueError(f"invalid compliance table pair {pair!r}")
        self._tc = current_table
        self._ts = snapshots_table
        self._snapshot_every_events = max(1, int(snapshot_every_events))
        if checkpoint_name:
            self.name = checkpoint_name

    async def _load_current(self, conn: asyncpg.Connection, app_id: str) -> dict[str, Any]:
        row = await conn.fetchrow(
            f"""
            SELECT regulation_set_version, checks, verdict, event_count, latest_event_at
            FROM {self._tc}
            WHERE application_id = $1
            """,
            app_id,
        )
        if row is None:
            return {
                "application_id": app_id,
                "regulation_set_version": None,
                "checks": [],
                "verdict": None,
                "event_count": 0,
                "latest_event_at": None,
            }
        return {
            "application_id": app_id,
            "regulation_set_version": row["regulation_set_version"],
            "checks": list(row["checks"] or []),
            "verdict": row["verdict"],
            "event_count": int(row["event_count"]),
            "latest_event_at": row["latest_event_at"],
        }

    async def _handle_with_conn(self, conn: asyncpg.Connection, event: StoredEvent) -> None:
        app_id = _app_id(event)
        if not app_id:
            return
        state = await self._load_current(conn, app_id)
        payload = event.payload
        event_at = _event_time(event)
        checks = list(state["checks"])
        if event.event_type == "ComplianceCheckInitiated":
            state["regulation_set_version"] = payload.get("regulation_set_version")
        elif event.event_type in {"ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted"}:
            checks.append(
                {
                    "event_type": event.event_type,
                    "rule_id": payload.get("rule_id"),
                    "rule_name": payload.get("rule_name"),
                    "rule_version": payload.get("rule_version"),
                    "evaluated_at": payload.get("evaluated_at"),
                    "status": (
                        "PASS"
                        if event.event_type == "ComplianceRulePassed"
                        else "FAIL"
                        if event.event_type == "ComplianceRuleFailed"
                        else "NOTE"
                    ),
                    "details": {
                        "failure_reason": payload.get("failure_reason"),
                        "is_hard_block": payload.get("is_hard_block"),
                        "note_text": payload.get("note_text"),
                    },
                }
            )
        elif event.event_type == "ComplianceCheckCompleted":
            state["verdict"] = payload.get("overall_verdict")
        state["checks"] = checks
        state["event_count"] = int(state["event_count"]) + 1
        state["latest_event_at"] = event_at

        await conn.execute(
                f"""
                INSERT INTO {self._tc}(
                    application_id,
                    regulation_set_version,
                    checks,
                    verdict,
                    latest_event_at,
                    last_event_type,
                    event_count
                )
                VALUES($1, $2, $3::jsonb, $4, $5, $6, $7)
                ON CONFLICT (application_id)
                DO UPDATE SET
                    regulation_set_version = EXCLUDED.regulation_set_version,
                    checks = EXCLUDED.checks,
                    verdict = EXCLUDED.verdict,
                    latest_event_at = EXCLUDED.latest_event_at,
                    last_event_type = EXCLUDED.last_event_type,
                    event_count = EXCLUDED.event_count
                """,
                app_id,
                state["regulation_set_version"],
                json.dumps(state["checks"]),
                state["verdict"],
                state["latest_event_at"],
                event.event_type,
                state["event_count"],
            )
        if state["event_count"] % self._snapshot_every_events == 0:
            await conn.execute(
                    f"""
                    INSERT INTO {self._ts}(
                        application_id, up_to_global_position, snapshot_at, snapshot_payload
                    )
                    VALUES ($1, $2, $3, $4::jsonb)
                    ON CONFLICT (application_id, up_to_global_position) DO NOTHING
                    """,
                    app_id,
                    event.global_position,
                    event_at,
                    json.dumps(
                        {
                            "application_id": app_id,
                            "regulation_set_version": state["regulation_set_version"],
                            "checks": state["checks"],
                            "verdict": state["verdict"],
                            "latest_event_at": (
                                state["latest_event_at"].isoformat()
                                if isinstance(state["latest_event_at"], datetime)
                                else state["latest_event_at"]
                            ),
                            "event_count": state["event_count"],
                        }
                    ),
                )

    async def get_current_compliance(self, application_id: str) -> dict[str, Any] | None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            state = await self._load_current(conn, application_id)
            return state if state["event_count"] > 0 else None

    async def get_compliance_at(self, application_id: str, timestamp: datetime) -> dict[str, Any] | None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            snap = await conn.fetchrow(
                f"""
                SELECT up_to_global_position, snapshot_payload
                FROM {self._ts}
                WHERE application_id = $1 AND snapshot_at <= $2
                ORDER BY up_to_global_position DESC
                LIMIT 1
                """,
                application_id,
                timestamp,
            )
            from_pos = 0
            if snap:
                state = dict(snap["snapshot_payload"])
                from_pos = int(snap["up_to_global_position"])
            else:
                state = {
                    "application_id": application_id,
                    "regulation_set_version": None,
                    "checks": [],
                    "verdict": None,
                    "latest_event_at": None,
                    "event_count": 0,
                }
            rows = await conn.fetch(
                """
                SELECT event_type, payload, recorded_at, global_position
                FROM events
                WHERE global_position > $1
                  AND stream_id = $2
                  AND recorded_at <= $3
                ORDER BY global_position ASC
                """,
                from_pos,
                f"compliance-{application_id}",
                timestamp,
            )
            for row in rows:
                e_type = str(row["event_type"])
                payload = coerce_jsonb_dict(row["payload"])
                if e_type == "ComplianceCheckInitiated":
                    state["regulation_set_version"] = payload.get("regulation_set_version")
                elif e_type in {"ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted"}:
                    checks = list(state.get("checks", []))
                    checks.append(
                        {
                            "event_type": e_type,
                            "rule_id": payload.get("rule_id"),
                            "rule_name": payload.get("rule_name"),
                            "rule_version": payload.get("rule_version"),
                            "evaluated_at": payload.get("evaluated_at"),
                        }
                    )
                    state["checks"] = checks
                elif e_type == "ComplianceCheckCompleted":
                    state["verdict"] = payload.get("overall_verdict")
                state["event_count"] = int(state.get("event_count", 0)) + 1
                state["latest_event_at"] = row["recorded_at"]
            return state if state["event_count"] > 0 else None

    async def _reset_state(self) -> None:
        pool = self._store._require_pool()
        async with pool.acquire() as conn:
            await conn.execute(f"TRUNCATE TABLE {self._tc}, {self._ts}")


async def swap_compliance_audit_read_models(conn: asyncpg.Connection) -> None:
    """Exchange primary and blue-green compliance tables (metadata locks only; keep calls short)."""
    await conn.execute("ALTER TABLE compliance_audit_current RENAME TO compliance_audit_cur__swap_tmp")
    await conn.execute("ALTER TABLE compliance_audit_current_bg RENAME TO compliance_audit_current")
    await conn.execute("ALTER TABLE compliance_audit_cur__swap_tmp RENAME TO compliance_audit_current_bg")
    await conn.execute("ALTER TABLE compliance_audit_snapshots RENAME TO compliance_audit_snap__swap_tmp")
    await conn.execute("ALTER TABLE compliance_audit_snapshots_bg RENAME TO compliance_audit_snapshots")
    await conn.execute("ALTER TABLE compliance_audit_snap__swap_tmp RENAME TO compliance_audit_snapshots_bg")


async def rebuild_compliance_audit_blue_green(
    store: EventStore,
    *,
    max_tail_rounds: int = 64,
) -> int:
    """Replay compliance into ``*_bg`` tables, chase the event tail, swap into primary.

    **Pause** :class:`ProjectionDaemon` (or stop projecting compliance events) before calling —
    otherwise the active daemon mutates primary ``compliance_audit_current`` while this runs and
    the swap would drop those rows. Event appends to ``events`` continue uninterrupted.

    Acknowledge with env ``LEDGER_COMPLIANCE_REBUILD_DAEMON_PAUSED=1``.
    """
    if os.environ.get("LEDGER_COMPLIANCE_REBUILD_DAEMON_PAUSED") != "1":
        raise RuntimeError(
            "Stop the projection daemon (or compliance leg) for this process, then set "
            "LEDGER_COMPLIANCE_REBUILD_DAEMON_PAUSED=1"
        )
    bg = ComplianceAuditViewProjection(
        store,
        current_table="compliance_audit_current_bg",
        snapshots_table="compliance_audit_snapshots_bg",
        checkpoint_name="compliance_audit_view_bg",
    )
    await bg.rebuild_from_scratch()
    pool = store._require_pool()
    last_applied = await bg.get_checkpoint()
    head = last_applied
    for _ in range(max(1, int(max_tail_rounds))):
        async with pool.acquire() as conn:
            head = int(await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events"))
        if head <= last_applied:
            break
        async for event in store.load_all(from_global_position=last_applied, batch_size=500):
            if event.event_type in bg.subscribed_event_types:
                await bg.apply_event_and_checkpoint(event)
            last_applied = event.global_position
    async with pool.acquire() as conn:
        async with conn.transaction():
            await swap_compliance_audit_read_models(conn)
            await conn.execute(
                """
                INSERT INTO projection_checkpoints(projection_name, last_position, updated_at)
                VALUES($1, $2, NOW())
                ON CONFLICT (projection_name)
                DO UPDATE SET last_position = EXCLUDED.last_position, updated_at = NOW()
                """,
                "compliance_audit_view",
                head,
            )
            await conn.execute(
                "DELETE FROM projection_checkpoints WHERE projection_name = $1",
                "compliance_audit_view_bg",
            )
            await conn.execute("TRUNCATE TABLE compliance_audit_current_bg, compliance_audit_snapshots_bg")
    return head
