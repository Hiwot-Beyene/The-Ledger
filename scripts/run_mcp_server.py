"""Run the Ledger MCP server over stdio (Claude Desktop, Cursor MCP, etc.).

Requires ``DATABASE_URL``, schema applied (`src/migrations/001_event_store.up.sql`),
and a running PostgreSQL instance.

Usage::

    cd /path/to/apex-fintech
    export DATABASE_URL=postgresql://postgres:apex@127.0.0.1:5432/apex_ledger
    python scripts/run_mcp_server.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT / "src", REPO_ROOT):
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)

from event_store import EventStore
from mcp.server import LedgerMCPService, build_fastmcp_server


async def _run() -> None:
    load_dotenv(REPO_ROOT / ".env")
    url = (os.environ.get("DATABASE_URL") or "").strip()
    if not url:
        raise SystemExit("DATABASE_URL must be set (copy .env.example to .env).")
    store = EventStore(url)
    await store.connect()
    try:
        service = LedgerMCPService(store)
        app = build_fastmcp_server(service)
        await app.run_stdio_async()
    finally:
        await store.close()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
