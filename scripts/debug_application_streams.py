"""
debug_application_streams.py

Usage:
  python scripts/debug_application_streams.py --application APEX-6280

Prints the event-type “line” status for all expected streams for an application:
  - loan-{id}
  - docpkg-{id}
  - credit-{id}
  - fraud-{id}
  - compliance-{id}

Also prints any DomainError / AgentSessionFailed events tied to the application_id.
"""

from __future__ import annotations

import argparse
import os
import asyncio
import sys
from typing import Any

from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for _p in (os.path.join(ROOT, "src"), ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from event_store import EventStore  # noqa: E402


def _event_type(e: Any) -> str:
    if isinstance(e, dict):
        return str(e.get("event_type") or "")
    return str(getattr(e, "event_type", "") or "")


def _payload(e: Any) -> dict[str, Any]:
    if isinstance(e, dict):
        p = e.get("payload")
        return p if isinstance(p, dict) else {}
    p = getattr(e, "payload", {})
    return p if isinstance(p, dict) else {}


def _recorded_at(e: Any) -> str:
    if isinstance(e, dict):
        return str(e.get("recorded_at") or "")
    return str(getattr(e, "recorded_at", "") or "")


def _print_stream_block(stream_id: str, events: list[Any]) -> None:
    print(f"\nstream {stream_id} count={len(events)}")
    if not events:
        return
    for e in events:
        et = _event_type(e)
        rec = _recorded_at(e)
        payload = _payload(e)
        keys = list(payload.keys())
        print(f" - {et} @ {rec} payload_keys={keys}")


async def main() -> None:
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

    ap = argparse.ArgumentParser()
    ap.add_argument("--application", required=True, help="application_id (e.g. APEX-6280)")
    args = ap.parse_args()

    app_id = args.application.strip()
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL not set (load .env expected)")

    store = EventStore(db_url)
    await store.connect()
    try:
        streams = [
            f"loan-{app_id}",
            f"docpkg-{app_id}",
            f"credit-{app_id}",
            f"fraud-{app_id}",
            f"compliance-{app_id}",
        ]

        print("=" * 100)
        print("Application debug:", app_id)
        print("=" * 100)

        for s in streams:
            events = await store.load_stream(s)
            _print_stream_block(s, events)

        # Pull out relevant failure signals across all streams, but only those for this application.
        failure_event_types = [
            "DomainError",
            "AgentSessionFailed",
            "AgentInputValidationFailed",
        ]

        failures_found = 0
        async for evt in store.load_all(event_types=failure_event_types):
            payload = _payload(evt)
            if str(payload.get("application_id") or "") != app_id:
                continue
            failures_found += 1
            print("\nFAILURE EVENT:", _event_type(evt), "recorded_at=", _recorded_at(evt))
            print(" payload:", payload)

        if failures_found == 0:
            print("\nNo DomainError / AgentSessionFailed events found for this application.")

    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())

