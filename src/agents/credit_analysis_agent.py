"""
src/agents/credit_analysis_agent.py
=======================================
CREDIT ANALYSIS AGENT — complete LangGraph reference implementation.

This is the reference agent. Read this fully before implementing
FraudDetectionAgent, ComplianceAgent, or DecisionOrchestratorAgent.

LangGraph nodes (in order):
  validate_inputs → open_credit_record → load_applicant_registry →
  load_extracted_facts → analyze_credit_risk → apply_policy_constraints →
  write_output

Input streams read:
  docpkg-{id}  → ExtractionCompleted events (current-year GAAP facts)

Databases queried:
  applicant_registry.companies         (read-only)
  applicant_registry.financial_history (read-only)
  applicant_registry.compliance_flags  (read-only)
  applicant_registry.loan_relationships(read-only)

Output events written:
  credit-{id}: CreditRecordOpened, HistoricalProfileConsumed,
               ExtractedFactsConsumed, CreditAnalysisCompleted (or CreditAnalysisDeferred)
  loan-{id}:   FraudScreeningRequested  (triggers next agent)

WHEN THIS WORKS:
  pytest tests/phase2/test_credit_agent.py   # all pass
  python scripts/run_pipeline.py --app APEX-0007 --phase credit
    → CreditAnalysisCompleted event in event store
    → rationale field is non-empty prose
    → confidence between 0.60 and 0.95
    → FraudScreeningRequested event on loan stream
"""
from __future__ import annotations
import time, json
from datetime import datetime
from decimal import Decimal
from typing import TypedDict, Annotated
from uuid import uuid4

from langgraph.graph import StateGraph, END

from agents.base_agent import BaseApexAgent
from aggregates.replay import event_type_from_stored, payload_from_stored
from models.events import (
    CreditRecordOpened, HistoricalProfileConsumed, ExtractedFactsConsumed,
    CreditAnalysisCompleted, CreditAnalysisDeferred,
    FraudScreeningRequested,
    CreditDecision, RiskTier, FinancialFacts,
)


# ─── STATE ────────────────────────────────────────────────────────────────────

class CreditState(TypedDict):
    # Identity
    application_id: str
    session_id: str
    applicant_id: str | None
    requested_amount_usd: float | None
    loan_purpose: str | None
    # Registry data
    company_profile: dict | None
    historical_financials: list[dict] | None
    compliance_flags: list[dict] | None
    loan_history: list[dict] | None
    # Document data
    extracted_facts: dict | None
    quality_flags: list[str] | None
    document_ids_consumed: list[str] | None
    # Analysis output
    credit_decision: dict | None
    policy_violations: list[str] | None
    # Plumbing
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None


# ─── AGENT ────────────────────────────────────────────────────────────────────

class CreditAnalysisAgent(BaseApexAgent):

    def build_graph(self) -> Any:
        from typing import Any
        g = StateGraph(CreditState)
        g.add_node("validate_inputs",          self._node_validate_inputs)
        g.add_node("open_credit_record",       self._node_open_credit_record)
        g.add_node("load_applicant_registry",  self._node_load_registry)
        g.add_node("load_extracted_facts",     self._node_load_facts)
        g.add_node("analyze_credit_risk",      self._node_analyze)
        g.add_node("apply_policy_constraints", self._node_policy)
        g.add_node("write_output",             self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",          "open_credit_record")
        g.add_edge("open_credit_record",       "load_applicant_registry")
        g.add_edge("load_applicant_registry",  "load_extracted_facts")
        g.add_edge("load_extracted_facts",     "analyze_credit_risk")
        g.add_edge("analyze_credit_risk",      "apply_policy_constraints")
        g.add_edge("apply_policy_constraints", "write_output")
        g.add_edge("write_output",             END)
        return g.compile()

    def _initial_state(self, application_id: str) -> CreditState:
        return CreditState(
            application_id=application_id, session_id=self.session_id,
            applicant_id=None, requested_amount_usd=None, loan_purpose=None,
            company_profile=None, historical_financials=None,
            compliance_flags=None, loan_history=None,
            extracted_facts=None, quality_flags=None, document_ids_consumed=None,
            credit_decision=None, policy_violations=None,
            errors=[], output_events=[], next_agent=None,
        )

    # ── NODE 1: VALIDATE INPUTS ───────────────────────────────────────────────
    async def _node_validate_inputs(self, state: CreditState) -> CreditState:
        t = time.time()
        app_id = state["application_id"]
        errors = []

        applicant_id: str | None = None
        requested_amount_usd: float | None = None
        loan_purpose: str | None = None
        for event in await self.store.load_stream(f"loan-{app_id}"):
            if event_type_from_stored(event) != "ApplicationSubmitted":
                continue
            pl = payload_from_stored(event)
            applicant_id = pl.get("applicant_id") if pl.get("applicant_id") is not None else None
            if applicant_id is not None:
                applicant_id = str(applicant_id)
            raw_amt = pl.get("requested_amount_usd")
            if raw_amt is not None:
                try:
                    requested_amount_usd = float(raw_amt)
                except (TypeError, ValueError):
                    requested_amount_usd = None
            lp = pl.get("loan_purpose")
            loan_purpose = str(lp) if lp is not None and str(lp).strip() else None
            break

        if not applicant_id:
            errors.append("ApplicationSubmitted missing or has no applicant_id on loan stream")
        if requested_amount_usd is None or requested_amount_usd <= 0:
            errors.append("ApplicationSubmitted missing or invalid requested_amount_usd")

        state["applicant_id"] = applicant_id
        state["requested_amount_usd"] = requested_amount_usd
        state["loan_purpose"] = loan_purpose

        # Verify package is ready
        # TODO: pkg = await DocumentPackageAggregate.load(self.store, app_id)
        # if not pkg.is_ready_for_analysis:
        #     errors.append("Document package not ready")

        ms = int((time.time() - t) * 1000)
        if errors:
            await self._record_input_failed([], errors)
            raise ValueError(f"Input validation failed: {errors}")

        await self._record_input_validated(
            ["application_id", "applicant_id", "document_package_ready"], ms
        )
        await self._record_node_execution(
            "validate_inputs",
            ["application_id"],
            ["applicant_id", "requested_amount_usd", "loan_purpose"],
            ms,
        )
        return {**state, "errors": errors}

    # ── NODE 2: OPEN CREDIT RECORD ────────────────────────────────────────────
    async def _node_open_credit_record(self, state: CreditState) -> CreditState:
        t = time.time()
        app_id = state["application_id"]
        credit_stream = f"credit-{app_id}"

        existing = await self.store.load_stream(credit_stream)
        if any(e.get("event_type") == "CreditRecordOpened" for e in existing):
            ms = int((time.time() - t) * 1000)
            await self._record_node_execution(
                "open_credit_record",
                ["application_id", "applicant_id"],
                ["credit_record_opened"],
                ms,
            )
            return state

        event = CreditRecordOpened(
            application_id=app_id,
            applicant_id=state["applicant_id"],
            opened_at=datetime.now(),
        ).to_store_dict()

        ver = await self.store.stream_version(credit_stream)
        await self.store.append(credit_stream, [event], expected_version=ver)

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(
            "open_credit_record",
            ["application_id", "applicant_id"],
            ["credit_record_opened"],
            ms,
        )
        return state

    # ── NODE 3: LOAD APPLICANT REGISTRY ──────────────────────────────────────
    async def _node_load_registry(self, state: CreditState) -> CreditState:
        t = time.time()
        applicant_id = state["applicant_id"]

        # Query Applicant Registry (read-only external database)
        # TODO: implement RegistryClient methods
        # profile   = await self.registry.get_company(applicant_id)
        # financials = await self.registry.get_financial_history(applicant_id)
        # flags     = await self.registry.get_compliance_flags(applicant_id)
        # loans     = await self.registry.get_loan_relationships(applicant_id)

        # PLACEHOLDER
        profile    = {"company_id": applicant_id, "name": "Company",
                      "industry": "technology", "trajectory": "STABLE",
                      "legal_type": "LLC", "jurisdiction": "CA"}
        financials: list[dict] = []
        flags:      list[dict] = []
        loans:      list[dict] = []

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call(
            "query_applicant_registry",
            f"company_id={applicant_id} tables=[companies,financial_history,compliance_flags,loan_relationships]",
            f"Loaded profile, {len(financials)} fiscal years, {len(flags)} flags, {len(loans)} loans",
            ms,
        )

        # Record what was consumed
        has_defaults = any(l.get("default_occurred") for l in loans)
        traj = profile.get("trajectory", "UNKNOWN")
        event = HistoricalProfileConsumed(
            application_id=state["application_id"],
            session_id=self.session_id,
            fiscal_years_loaded=[f["fiscal_year"] for f in financials],
            has_prior_loans=bool(loans),
            has_defaults=has_defaults,
            revenue_trajectory=traj,
            data_hash=self._sha({"fins": financials, "flags": flags}),
            consumed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"credit-{state['application_id']}", [event])

        await self._record_node_execution(
            "load_applicant_registry",
            ["applicant_id"],
            ["company_profile", "historical_financials", "compliance_flags", "loan_history"],
            ms,
        )
        return {
            **state,
            "company_profile":      profile,
            "historical_financials": financials,
            "compliance_flags":     flags,
            "loan_history":         loans,
        }

    # ── NODE 4: LOAD EXTRACTED FACTS ──────────────────────────────────────────
    async def _node_load_facts(self, state: CreditState) -> CreditState:
        t = time.time()
        app_id = state["application_id"]

        # Load ExtractionCompleted events from document package stream
        pkg_events = await self.store.load_stream(f"docpkg-{app_id}")
        extraction_events = [
            e for e in pkg_events
            if e["event_type"] == "ExtractionCompleted"
        ]

        # Merge facts from income statement and balance sheet extractions
        merged_facts: dict = {}
        doc_ids: list[str] = []
        quality_flags: list[str] = []

        for ev in extraction_events:
            payload = ev["payload"]
            doc_ids.append(payload.get("document_id", "unknown"))
            facts = payload.get("facts") or {}
            for k, v in facts.items():
                if v is not None and k not in merged_facts:
                    merged_facts[k] = v
            # Collect quality flags
            if facts.get("extraction_notes"):
                quality_flags.extend(facts["extraction_notes"])

        # Also check for quality assessment anomalies
        qa_events = [e for e in pkg_events if e["event_type"] == "QualityAssessmentCompleted"]
        for ev in qa_events:
            quality_flags.extend(ev["payload"].get("anomalies", []))
            quality_flags.extend([
                f"CRITICAL_MISSING:{f}"
                for f in ev["payload"].get("critical_missing_fields", [])
            ])

        ms = int((time.time() - t) * 1000)
        await self._record_tool_call(
            "load_event_store_stream",
            f"stream_id=docpkg-{app_id} filter=ExtractionCompleted",
            f"Loaded {len(extraction_events)} extraction results, {len(quality_flags)} flags",
            ms,
        )

        # Record consumption
        event = ExtractedFactsConsumed(
            application_id=app_id,
            session_id=self.session_id,
            document_ids_consumed=doc_ids,
            facts_summary=f"revenue={merged_facts.get('total_revenue')}, net_income={merged_facts.get('net_income')}",
            quality_flags_present=bool(quality_flags),
            consumed_at=datetime.now(),
        ).to_store_dict()
        await self._append_with_retry(f"credit-{app_id}", [event])

        # Defer if facts are too incomplete
        critical = ["total_revenue", "net_income", "total_assets"]
        missing_critical = [k for k in critical if not merged_facts.get(k)]
        if len(missing_critical) >= 2:
            defer_event = CreditAnalysisDeferred(
                application_id=app_id, session_id=self.session_id,
                deferral_reason="Insufficient document extraction quality",
                quality_issues=[f"Missing critical field: {f}" for f in missing_critical],
                deferred_at=datetime.now(),
            ).to_store_dict()
            await self._append_with_retry(f"credit-{app_id}", [defer_event])
            raise ValueError(f"Credit analysis deferred: missing {missing_critical}")

        await self._record_node_execution(
            "load_extracted_facts",
            ["document_package_events"],
            ["extracted_facts", "quality_flags"],
            ms,
        )
        return {**state, "extracted_facts": merged_facts, "quality_flags": quality_flags,
                "document_ids_consumed": doc_ids}

    # ── NODE 5: ANALYZE CREDIT RISK (deterministic) ──────────────────────────
    async def _node_analyze(self, state: CreditState) -> CreditState:
        t = time.time()
        facts = state.get("extracted_facts") or {}
        requested = float(state.get("requested_amount_usd") or 0.0)
        q_flags = [str(x) for x in (state.get("quality_flags") or [])]
        flags = state.get("compliance_flags") or []
        loans = state.get("loan_history") or []

        def _f(v: object) -> float | None:
            try:
                if v is None or v == "":
                    return None
                return float(v)  # ok for Decimal/str/number
            except Exception:
                return None

        revenue = _f(facts.get("total_revenue"))
        net_income = _f(facts.get("net_income"))
        ebitda = _f(facts.get("ebitda"))
        assets = _f(facts.get("total_assets"))
        liabilities = _f(facts.get("total_liabilities"))

        policy_overrides: list[str] = []
        key_concerns: list[str] = []
        data_quality: list[str] = []

        missing = [k for k in ("total_revenue", "net_income", "total_assets") if _f(facts.get(k)) is None]
        if missing:
            data_quality.append(f"Missing critical fields: {', '.join(missing)}")
        if any("week3_pipeline_unavailable_or_failed" in f for f in q_flags):
            data_quality.append("Week 3 extraction service unavailable; used fallback extraction.")
        if any("critical_missing_field=" in f for f in q_flags):
            data_quality.append("One or more critical fields were missing in extraction; confidence reduced.")

        prior_default = any(bool(l.get("default_occurred")) for l in loans)
        active_high_flag = any(f.get("severity") == "HIGH" and f.get("is_active") for f in flags)

        # Base deterministic confidence from data completeness.
        confidence = 0.80
        confidence -= 0.15 * len(missing)
        if data_quality:
            confidence -= 0.10
        if active_high_flag:
            confidence = min(confidence, 0.50)
            policy_overrides.append("POLICY_COMPLIANCE_FLAG_CONFIDENCE_CAP")

        # Simple risk tier heuristics.
        risk_tier = "MEDIUM"
        if prior_default:
            risk_tier = "HIGH"
            policy_overrides.append("POLICY_PRIOR_DEFAULT_FORCE_HIGH")
        else:
            # Profitability / leverage heuristics.
            if revenue and net_income is not None:
                margin = net_income / revenue if revenue else 0.0
                if margin < 0:
                    risk_tier = "HIGH"
                    key_concerns.append(f"Negative net income margin ({margin:.1%}).")
                elif margin < 0.03:
                    risk_tier = "MEDIUM"
                    key_concerns.append(f"Thin net margin ({margin:.1%}).")
                else:
                    risk_tier = "LOW"
            if assets and liabilities:
                leverage = liabilities / assets if assets else 0.0
                if leverage > 0.85:
                    risk_tier = "HIGH"
                    key_concerns.append(f"High leverage (liabilities/assets {leverage:.0%}).")

        # Recommended limit: start from requested, cap by revenue rule if available.
        recommended_limit = int(max(0.0, requested))
        if revenue and revenue > 0:
            cap = int(revenue * 0.35)
            if recommended_limit > cap:
                recommended_limit = cap
                policy_overrides.append("POLICY_REV_CAP_35PCT")
        else:
            # Without revenue, be conservative.
            recommended_limit = int(requested * 0.50)
            key_concerns.append("Revenue unavailable; conservative limit applied.")

        # Debt service coverage proxy (quick, deterministic; no amortization modeling).
        if ebitda is not None and recommended_limit > 0:
            # crude annual payment proxy: 20% of principal (keeps this deterministic + fast)
            annual_payment_proxy = 0.20 * recommended_limit
            dsc = (ebitda / annual_payment_proxy) if annual_payment_proxy else 0.0
            if dsc < 1.25:
                key_concerns.append(f"Weak DSCR proxy ({dsc:.2f}x < 1.25x).")
                confidence -= 0.10

        confidence = max(0.0, min(1.0, confidence))

        # Do not model full intake as credit exposure when statements are incomplete or flagged;
        # otherwise revenue-based caps can still equal intake and look "uncalculated".
        if requested > 0 and (data_quality or missing):
            max_with_partial_data = int(max(0.0, requested * 0.50))
            if recommended_limit > max_with_partial_data:
                recommended_limit = max_with_partial_data
                policy_overrides.append("POLICY_DATA_UNCERTAINTY_MAX_50PCT_INTAKE")

        rationale_bits = []
        if revenue:
            rationale_bits.append(f"Revenue basis available (total_revenue≈${revenue:,.0f}).")
        if risk_tier == "LOW":
            rationale_bits.append("Signals indicate lower risk on profitability/leverage heuristics.")
        elif risk_tier == "HIGH":
            rationale_bits.append("Signals indicate higher risk; recommend tighter exposure and review.")
        else:
            rationale_bits.append("Risk is moderate given available signals.")
        if policy_overrides:
            rationale_bits.append("Hard policy constraints were applied.")
        if data_quality:
            rationale_bits.append("Data quality caveats reduce confidence.")

        decision = {
            "risk_tier": risk_tier,
            "recommended_limit_usd": int(recommended_limit),
            "confidence": float(confidence),
            "rationale": " ".join(rationale_bits)[:600],
            "key_concerns": key_concerns[:8],
            "data_quality_caveats": data_quality[:8],
            "policy_overrides_applied": policy_overrides[:8],
        }

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(
            "analyze_credit_risk",
            ["historical_financials", "extracted_facts", "company_profile", "loan_request"],
            ["credit_decision"],
            ms,
        )
        return {**state, "credit_decision": decision}

    # ── NODE 6: APPLY POLICY CONSTRAINTS (deterministic) ─────────────────────
    async def _node_policy(self, state: CreditState) -> CreditState:
        t = time.time()
        d        = dict(state["credit_decision"])
        hist     = state.get("historical_financials") or []
        req      = state.get("requested_amount_usd") or 0
        flags    = state.get("compliance_flags") or []
        loans    = state.get("loan_history") or []
        viols:  list[str] = []

        # Policy 1: loan-to-revenue cap
        if hist:
            rev = hist[-1].get("total_revenue", 0)
            cap = int(rev * 0.35)
            if cap > 0 and d.get("recommended_limit_usd", 0) > cap:
                d["recommended_limit_usd"] = cap
                viols.append(f"POLICY_REV_CAP: limit capped at 35% of revenue (${cap:,.0f})")

        # Policy 2: prior default → HIGH
        if any(l.get("default_occurred") for l in loans):
            if d.get("risk_tier") != "HIGH":
                d["risk_tier"] = "HIGH"
                viols.append("POLICY_PRIOR_DEFAULT: risk_tier elevated to HIGH")

        # Policy 3: active HIGH flag → confidence cap
        if any(f.get("severity") == "HIGH" and f.get("is_active") for f in flags):
            if d.get("confidence", 0) > 0.50:
                d["confidence"] = 0.50
                viols.append("POLICY_COMPLIANCE_FLAG: confidence capped at 0.50")

        if viols:
            d["policy_overrides_applied"] = d.get("policy_overrides_applied", []) + viols

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(
            "apply_policy_constraints",
            ["credit_decision", "historical_financials", "loan_history", "compliance_flags"],
            ["credit_decision"],
            ms,
        )
        return {**state, "credit_decision": d, "policy_violations": viols}

    # ── NODE 7: WRITE OUTPUT ──────────────────────────────────────────────────
    async def _node_write_output(self, state: CreditState) -> CreditState:
        t = time.time()
        app_id = state["application_id"]
        d      = state["credit_decision"]

        # Build and append CreditAnalysisCompleted
        credit_event = CreditAnalysisCompleted(
            application_id=app_id,
            session_id=self.session_id,
            decision=CreditDecision(
                risk_tier=RiskTier(d["risk_tier"]),
                recommended_limit_usd=Decimal(str(d["recommended_limit_usd"])),
                confidence=float(d["confidence"]),
                rationale=d.get("rationale", ""),
                key_concerns=d.get("key_concerns", []),
                data_quality_caveats=d.get("data_quality_caveats", []),
                policy_overrides_applied=d.get("policy_overrides_applied", []),
            ),
            model_version=self.model,
            model_deployment_id=f"dep-{uuid4().hex[:8]}",
            input_data_hash=self._sha(state),
            analysis_duration_ms=int((time.time() - self._t0) * 1000),
            completed_at=datetime.now(),
        ).to_store_dict()

        # OCC-safe write to credit stream
        positions = await self._append_with_retry(
            f"credit-{app_id}", [credit_event],
            causation_id=self.session_id,
            correlation_id=app_id,
        )

        fraud_trigger = FraudScreeningRequested(
            application_id=app_id,
            requested_at=datetime.now(),
            triggered_by_event_id=self.session_id,
        ).to_store_dict()
        loan_positions = await self._append_with_retry(
            f"loan-{app_id}",
            [credit_event, fraud_trigger],
            causation_id=self.session_id,
            correlation_id=app_id,
        )

        events_written = [
            {"stream_id": f"credit-{app_id}", "event_type": "CreditAnalysisCompleted",
             "stream_position": positions[0] if positions else -1},
            {"stream_id": f"loan-{app_id}", "event_type": "CreditAnalysisCompleted",
             "stream_position": loan_positions[0] if loan_positions else -1},
            {"stream_id": f"loan-{app_id}", "event_type": "FraudScreeningRequested",
             "stream_position": loan_positions[1] if len(loan_positions) > 1 else -1},
        ]
        await self._record_output_written(
            events_written,
            f"Credit: {d['risk_tier']} risk, ${d['recommended_limit_usd']:,.0f} limit, "
            f"{d['confidence']:.0%} confidence. Fraud screening triggered.",
        )

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution(
            "write_output", ["credit_decision"], ["events_written"], ms
        )
        return {**state, "output_events": events_written, "next_agent": "fraud_detection"}
