from datetime import UTC, datetime

from models.event_factories import (
    store_dict_application_submitted,
    store_dict_compliance_check_requested,
    store_dict_credit_analysis_completed_for_loan_stream,
    store_dict_credit_analysis_requested,
    store_dict_decision_generated_for_loan_stream,
    store_dict_decision_requested,
)
from models.events import DecisionRecommendation, deserialize_event


def test_factories_roundtrip_through_deserialize_event():
    t = datetime.now(UTC)
    d_app = store_dict_application_submitted("A1", "C1", 100_000, submitted_at=t)
    ev_app = deserialize_event(d_app["event_type"], d_app["payload"])
    assert ev_app.application_id == "A1"
    assert ev_app.applicant_id == "C1"

    d_credit = store_dict_credit_analysis_completed_for_loan_stream(
        application_id="A1",
        agent_id="credit",
        session_id="s1",
        model_version="v1",
        confidence_score=0.9,
        risk_tier="LOW",
        recommended_limit_usd=50_000,
        analysis_duration_ms=100,
        input_data_hash="abc",
        completed_at=t,
    )
    ev_c = deserialize_event(d_credit["event_type"], d_credit["payload"])
    assert ev_c.agent_id == "credit"
    assert ev_c.decision is None
    assert ev_c.confidence_score == 0.9

    d_dec = store_dict_decision_generated_for_loan_stream(
        application_id="A1",
        recommendation="REFER",
        confidence_score=0.4,
        contributing_agent_sessions=["agent-credit-s1"],
        executive_summary="sum",
        generated_at=t,
    )
    ev_d = deserialize_event(d_dec["event_type"], d_dec["payload"])
    assert ev_d.confidence_score == 0.4
    assert ev_d.contributing_agent_sessions == ["agent-credit-s1"]
    assert ev_d.recommendation is DecisionRecommendation.REFER

    d_req = store_dict_credit_analysis_requested("A2", requested_by="svc", requested_at=t)
    assert deserialize_event(d_req["event_type"], d_req["payload"]).application_id == "A2"

    d_comp = store_dict_compliance_check_requested(
        "A2",
        triggered_by_event_id="e1",
        regulation_set_version="2026-Q1",
        rules_to_evaluate=["r1"],
        requested_at=t,
    )
    assert deserialize_event(d_comp["event_type"], d_comp["payload"]).rules_to_evaluate == ["r1"]

    d_dec_r = store_dict_decision_requested("A2", triggered_by_event_id="e2", requested_at=t)
    assert deserialize_event(d_dec_r["event_type"], d_dec_r["payload"]).all_analyses_complete is True
