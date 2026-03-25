"""Guarantee EVENT_REGISTRY stays aligned with BaseEvent subclasses in models.events."""

from models.events import validate_event_registry


def test_event_registry_covers_all_domain_events():
    validate_event_registry()
