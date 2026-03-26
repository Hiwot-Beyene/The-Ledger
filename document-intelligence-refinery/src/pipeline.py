from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
import re
import os
from typing import Any

from src.agents.extractor import ExtractionRouter
from src.agents.triage import TriageAgent
from src.models import StrategyName
from src.services.numeric_parser import parse_numeric
from src.utils.rules import load_rules


CRITICAL_FIELDS = (
    "total_revenue",
    "net_income",
    "total_assets",
    "total_liabilities",
)

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "total_revenue": ("total_revenue", "revenue", "sales", "turnover"),
    "gross_profit": ("gross_profit",),
    "operating_expenses": ("operating_expenses", "opex"),
    "operating_income": ("operating_income", "operating_profit"),
    "ebitda": ("ebitda",),
    "depreciation_amortization": ("depreciation_amortization", "depreciation", "amortization"),
    "interest_expense": ("interest_expense", "finance_cost"),
    "income_before_tax": ("income_before_tax", "profit_before_tax", "pbt"),
    "tax_expense": ("tax_expense", "income_tax"),
    "net_income": ("net_income", "net_profit", "profit_after_tax", "pat"),
    "total_assets": ("total_assets", "assets"),
    "current_assets": ("current_assets",),
    "cash_and_equivalents": ("cash_and_equivalents", "cash"),
    "accounts_receivable": ("accounts_receivable", "receivables"),
    "inventory": ("inventory", "inventories"),
    "total_liabilities": ("total_liabilities", "liabilities"),
    "current_liabilities": ("current_liabilities",),
    "long_term_debt": ("long_term_debt", "non_current_borrowings"),
    "total_equity": ("total_equity", "equity", "shareholders_equity"),
    "operating_cash_flow": ("operating_cash_flow", "cash_from_operations"),
    "investing_cash_flow": ("investing_cash_flow",),
    "financing_cash_flow": ("financing_cash_flow",),
    "free_cash_flow": ("free_cash_flow",),
}

INCOME_STATEMENT_PREFERRED = {
    "total_revenue",
    "gross_profit",
    "operating_expenses",
    "operating_income",
    "ebitda",
    "depreciation_amortization",
    "interest_expense",
    "income_before_tax",
    "tax_expense",
    "net_income",
}

BALANCE_SHEET_PREFERRED = {
    "total_assets",
    "current_assets",
    "cash_and_equivalents",
    "accounts_receivable",
    "inventory",
    "total_liabilities",
    "current_liabilities",
    "long_term_debt",
    "total_equity",
}


def _normalize_key(raw: str) -> str:
    key = (raw or "").strip().lower()
    key = re.sub(r"[^a-z0-9]+", "_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip()
    if not text:
        return None
    parsed = parse_numeric(text)
    if not parsed:
        return None
    try:
        return Decimal(str(parsed[0]))
    except (InvalidOperation, ValueError):
        return None


def _field_from_label(label: str) -> str | None:
    normalized = _normalize_key(label)
    for field, aliases in FIELD_ALIASES.items():
        if normalized in aliases:
            return field
    return None


def _extract_from_text_line(line: str) -> tuple[str, Decimal] | None:
    line = (line or "").strip()
    if not line:
        return None
    # Works for `Label: value` and `Label | value`.
    parts = re.split(r"\s*[:|]\s*", line, maxsplit=1)
    if len(parts) != 2:
        return None
    field = _field_from_label(parts[0])
    if field is None:
        return None
    value = _to_decimal(parts[1])
    if value is None:
        return None
    return field, value


def _preferred_fields(document_type: str) -> set[str]:
    dt = (document_type or "").strip().lower()
    if dt == "income_statement":
        return INCOME_STATEMENT_PREFERRED
    if dt == "balance_sheet":
        return BALANCE_SHEET_PREFERRED
    return set(FIELD_ALIASES.keys())


def extract_financial_facts(
    file_path: str,
    document_type: str,
    *,
    rules_path: str = "rubric/extraction_rules.yaml",
) -> dict[str, Any]:
    """
    Extract normalized financial facts from a PDF using the extraction router.

    Returns a dict that matches the FinancialFacts payload shape used by the
    ledger service (values as decimal-safe strings/None + quality metadata).
    """
    rules = load_rules(rules_path)
    resolved_path = _resolve_input_path(file_path)

    # Deterministic-by-default for Week 5 financial facts extraction:
    # - avoid Strategy C (vision/VLM) unless explicitly enabled
    # - still allow A -> B escalation for hard layouts
    allow_vision = os.getenv("REFINERY_FINANCIAL_FACTS_ALLOW_VISION", "0").strip().lower() in ("1", "true", "yes", "y")
    if not allow_vision:
        rules = dict(rules)
        rules["confidence"] = dict(rules.get("confidence", {}))
        # If we reach Strategy B, stop there (never escalate to C).
        rules["confidence"]["escalate_threshold_bc"] = -1.0

    router = ExtractionRouter(rules)
    profile = None
    if not allow_vision:
        triage = TriageAgent(rules)
        profile = triage.profile_document(resolved_path, persist=True)
        if getattr(profile, "selected_strategy", None) == StrategyName.C:
            profile.selected_strategy = StrategyName.B

    extracted, ledger = router.run(resolved_path, profile=profile)

    preferred = _preferred_fields(document_type)
    values: dict[str, Decimal] = {}

    ldus = extracted.get("ldus") or []
    for ldu in ldus:
        text = (ldu.get("text") or "").strip()
        if not text:
            continue
        for raw_line in text.splitlines():
            parsed = _extract_from_text_line(raw_line)
            if not parsed:
                continue
            field, value = parsed
            if field not in preferred:
                continue
            values.setdefault(field, value)

    field_confidence = {k: (1.0 if k in values else 0.0) for k in FIELD_ALIASES}
    extraction_notes = [
        f"document_type={document_type}",
        f"strategy_sequence={','.join(s.value for s in getattr(ledger, 'strategy_sequence', []))}",
        f"final_strategy={getattr(getattr(ledger, 'final_strategy', None), 'value', getattr(ledger, 'final_strategy', 'unknown'))}",
    ]

    for field in CRITICAL_FIELDS:
        if field not in values:
            field_confidence[field] = 0.0
            extraction_notes.append(f"critical_missing_field={field}")

    payload: dict[str, Any] = {k: None for k in FIELD_ALIASES}
    payload.update({k: str(v) for k, v in values.items()})
    payload["field_confidence"] = field_confidence
    payload["extraction_notes"] = extraction_notes
    payload["currency"] = "USD"
    payload["gaap_compliant"] = True
    payload["balance_sheet_balances"] = None
    payload["balance_discrepancy_usd"] = None

    return payload


def _resolve_input_path(file_path: str) -> Path:
    candidate = Path(file_path)
    if candidate.exists():
        return candidate

    # Common case: Week 5 stores paths like documents/COMP-001/file.pdf relative
    # to the apex-fintech repo root while this service runs from document-intelligence-refinery/.
    service_root = Path(__file__).resolve().parents[1]
    workspace_root = service_root.parent
    workspace_candidate = workspace_root / file_path
    if workspace_candidate.exists():
        return workspace_candidate

    raise FileNotFoundError(f"Input file not found: {file_path}")
