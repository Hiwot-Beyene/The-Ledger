"""Unit tests for orchestrator confidence normalization and intake-vs-cap capping."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from agents.stub_agents import (
    _cap_confidence_intake_vs_modeled_cap,
    _credit_confidence_from_payload,
    _normalize_unit_interval,
    _orchestrator_synthesis_confidence,
)


def test_normalize_unit_interval_percent_strings():
    assert _normalize_unit_interval("72%") == 0.72
    assert _normalize_unit_interval(" 65 ") == 0.65
    assert _normalize_unit_interval(72) == 0.72
    assert _normalize_unit_interval(0.65) == 0.65


def test_credit_confidence_from_nested_payload():
    p = {
        "decision": {"confidence": "72%", "risk_tier": "MEDIUM"},
        "confidence_score": 0.5,
    }
    assert _credit_confidence_from_payload(p) == 0.72


def test_credit_confidence_fallback_top_level():
    p = {"confidence_score": "55", "decision": {}}
    assert _credit_confidence_from_payload(p) == 0.55


def test_orchestrator_synthesis_min_llm_and_credit():
    c = {"confidence": 0.55}
    assert _orchestrator_synthesis_confidence({"confidence": 0.9}, c) == 0.55
    assert _orchestrator_synthesis_confidence({"confidence": 0.4}, c) == 0.4


def test_intake_cap_lowers_confidence_when_cap_far_below_ask():
    lo, did = _cap_confidence_intake_vs_modeled_cap(0.65, 80_000_000, 3_283_273)
    assert did is True
    assert lo < 0.50
    same, did2 = _cap_confidence_intake_vs_modeled_cap(0.65, 100_000, 99_000)
    assert did2 is False
    assert same == 0.65


def test_hitl_policy_not_forced_by_refer_above_floor(monkeypatch):
    from src.api.app import _hitl_confidence_policy

    monkeypatch.delenv("LEDGER_AUTO_BIND_HIGH_CONFIDENCE", raising=False)
    by = {"DecisionGenerated": {"payload": {"recommendation": "REFER", "confidence_score": 0.65}}}
    _thr, need = _hitl_confidence_policy(by)
    assert need is False

    monkeypatch.setenv("LEDGER_AUTO_BIND_HIGH_CONFIDENCE", "0")
    _thr2, need2 = _hitl_confidence_policy(by)
    assert need2 is True

    by_low = {"DecisionGenerated": {"payload": {"recommendation": "APPROVE", "confidence_score": 0.55}}}
    monkeypatch.delenv("LEDGER_AUTO_BIND_HIGH_CONFIDENCE", raising=False)
    assert _hitl_confidence_policy(by_low)[1] is True
