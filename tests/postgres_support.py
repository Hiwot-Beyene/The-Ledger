"""Helpers for optional PostgreSQL integration tests (EventStore).

Tests skip cleanly when no server is reachable — no failing collection errors.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "ledger" / "schema.sql"


def get_postgres_test_url() -> str:
    return (
        os.environ.get("APEX_LEDGER_TEST_DB_URL")
        or os.environ.get("TEST_DB_URL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:apex@127.0.0.1:5432/apex_ledger_test"
    )


def redact_url(url: str) -> str:
    if "@" not in url:
        return url
    head, tail = url.rsplit("@", 1)
    if "://" in head:
        scheme, rest = head.split("://", 1)
        if ":" in rest:
            user, _ = rest.split(":", 1)
            return f"{scheme}://{user}:***@{tail}"
    return url


def _sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        buf.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buf))
            buf.clear()
    if buf:
        statements.append("\n".join(buf))
    return statements


async def ensure_event_store_schema(pool) -> None:
    import asyncpg

    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'events'
            LIMIT 1
            """
        )
        if exists:
            return
        raw = SCHEMA_PATH.read_text(encoding="utf-8")
        for stmt in _sql_statements(raw):
            s = stmt.strip()
            if not s:
                continue
            try:
                await conn.execute(s)
            except asyncpg.PostgresError as e:
                if getattr(e, "sqlstate", None) == "42P07":
                    continue
                raise


async def truncate_event_store_tables(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE outbox, events, event_streams, projection_checkpoints
            RESTART IDENTITY CASCADE
            """
        )
