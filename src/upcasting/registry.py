"""Read-time upcaster registry. Upcaster bodies live in ``upcasting.upcasters``."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

UpcasterFn = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]


class UpcasterRegistry:
    """Apply on load paths only. Never call during append/write."""

    def __init__(self) -> None:
        self._upcasters: dict[tuple[str, int], UpcasterFn] = {}
        from upcasting.upcasters import register_builtin_upcasters

        register_builtin_upcasters(self)

    def register(self, event_type: str, from_version: int) -> Callable[[UpcasterFn], UpcasterFn]:
        def decorator(fn: UpcasterFn) -> UpcasterFn:
            self._upcasters[(event_type, int(from_version))] = fn
            return fn

        return decorator

    def upcast(self, event: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
        current = dict(event)
        current["payload"] = dict(current.get("payload", {}))
        ctx = context or {}
        event_type = str(current.get("event_type", ""))
        version = int(current.get("event_version", 1))
        while (event_type, version) in self._upcasters:
            fn = self._upcasters[(event_type, version)]
            current["payload"] = fn(dict(current["payload"]), {"event": current, **ctx})
            version += 1
            current["event_version"] = version
        return current
