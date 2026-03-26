"""Custom exceptions for extraction and budget guard."""

from __future__ import annotations


class BudgetApprovalRequired(Exception):
    """Raised when estimated vision cost exceeds budget and require_approval_over_budget is true."""

    def __init__(self, estimated_cost_usd: float, budget_cap_usd: float, page_count: int):
        self.estimated_cost_usd = estimated_cost_usd
        self.budget_cap_usd = budget_cap_usd
        self.page_count = page_count
        super().__init__(
            f"Estimated cost ${estimated_cost_usd:.2f} exceeds budget ${budget_cap_usd:.2f} for {page_count} pages. User approval required."
        )
