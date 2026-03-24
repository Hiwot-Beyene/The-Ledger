from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

logger = logging.getLogger("ledger.commands")


async def run_with_command_observability(
    handler_name: str,
    cmd: Any,
    body: Callable[[], Awaitable[None]],
) -> None:
    correlation_id = getattr(cmd, "correlation_id", None)
    application_id = getattr(cmd, "application_id", None)
    logger.info(
        "handler_start %s correlation_id=%r application_id=%r",
        handler_name,
        correlation_id,
        application_id,
    )
    t0 = time.perf_counter()
    try:
        await body()
    except Exception:
        logger.exception(
            "handler_failed %s correlation_id=%r application_id=%r",
            handler_name,
            correlation_id,
            application_id,
        )
        raise
    ms = (time.perf_counter() - t0) * 1000
    logger.info(
        "handler_ok %s duration_ms=%.3f correlation_id=%r application_id=%r",
        handler_name,
        ms,
        correlation_id,
        application_id,
    )
