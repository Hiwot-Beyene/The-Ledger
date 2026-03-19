# Schema layer (`ledger/schema`)

- **`events.py`** — Pydantic `BaseEvent` types, `EVENT_REGISTRY`, `deserialize_event`, `DomainError` / `OptimisticConcurrencyError` with `to_diagnostic_dict()` for APIs and logs.
- **`event_factories.py`** — Validated `store_dict_*` helpers for command → append payloads (keeps shapes aligned with models).
- **Invariants (examples)** — `ApplicationSubmitted.requested_amount_usd` &gt; 0; `DecisionGenerated.recommendation` ∈ {APPROVE, DECLINE, REFER}; `confidence_score` ∈ [0, 1]; `CreditAnalysisCompleted.risk_tier` matches `RiskTier` when set.

CI: `tests/test_event_registry.py`, `tests/test_event_factories.py`, `tests/test_domain_event_models.py`.
