from __future__ import annotations

from datetime import UTC, datetime

from navigator_query import parse_navigator_decision_history_query


def test_canonical_show_me_phrase() -> None:
    o = parse_navigator_decision_history_query(
        "Show me the complete decision history of application ID APEX-0042",
    )
    assert o.error is None
    assert o.application_id == "APEX-0042"
    assert o.intent_matched is True


def test_alternate_wording_audit_trail() -> None:
    o = parse_navigator_decision_history_query(
        "Pull the full audit trail and event stream for app id phase6-app-2 please",
    )
    assert o.error is None
    assert o.application_id == "phase6-app-2"


def test_loan_stream_prefix_stripped() -> None:
    o = parse_navigator_decision_history_query(
        "I need the regulatory package for loan-APEX-0001 — complete history",
    )
    assert o.error is None
    assert o.application_id == "APEX-0001"


def test_examination_as_of_iso() -> None:
    o = parse_navigator_decision_history_query(
        "Display decision history for application ID X-99 as of 2025-01-15T14:30:00Z",
    )
    assert o.error is None
    assert o.application_id == "X-99"
    assert o.examination_at == datetime(2025, 1, 15, 14, 30, 0, tzinfo=UTC)


def test_rejects_chitchat_with_id_token() -> None:
    o = parse_navigator_decision_history_query("APEX-0001 is my favourite year")
    assert o.error is not None
    assert "audit trail" in o.error.lower() or "decision history" in o.error.lower()


def test_rejects_missing_id() -> None:
    o = parse_navigator_decision_history_query("Show me the complete decision history right now")
    assert o.error is not None
    assert "application id" in o.error.lower()


def test_ambiguous_multiple_structured_ids() -> None:
    o = parse_navigator_decision_history_query(
        "History for application ID APEX-1 and also application ID APEX-2",
    )
    assert o.error is not None
    assert "multiple" in o.error.lower()
