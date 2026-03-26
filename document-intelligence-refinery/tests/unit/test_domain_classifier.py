from src.agents.domain_classifier import (
    DomainClassifier,
    create_domain_classifier,
    register_domain_classifier,
)
from src.models.common import DomainHint


class FinanceOnlyClassifier(DomainClassifier):
    def classify(self, text: str) -> DomainHint:
        return DomainHint.FINANCIAL


def test_classifier_registry_supports_custom_classifier():
    register_domain_classifier("finance-only", FinanceOnlyClassifier)
    classifier = create_domain_classifier("finance-only")
    assert classifier.classify("anything") == DomainHint.FINANCIAL


def test_unknown_classifier_falls_back_to_keyword():
    classifier = create_domain_classifier("missing")
    result = classifier.classify("this page includes tax policy details")
    assert result == DomainHint.FINANCIAL
