from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable

from src.models.common import DomainHint


DOMAIN_KEYWORDS = {
    DomainHint.FINANCIAL: ["revenue", "asset", "liability", "income", "tax", "fiscal"],
    DomainHint.LEGAL: ["plaintiff", "defendant", "clause", "statute", "compliance"],
    DomainHint.TECHNICAL: ["architecture", "system", "algorithm", "implementation", "api"],
    DomainHint.MEDICAL: ["diagnosis", "patient", "clinical", "medication", "hospital"],
}


class DomainClassifier(ABC):
    @abstractmethod
    def classify(self, text: str) -> DomainHint:
        ...


class KeywordDomainClassifier(DomainClassifier):
    def classify(self, text: str) -> DomainHint:
        haystack = (text or "").lower()
        scored = {domain: 0 for domain in DOMAIN_KEYWORDS}
        for domain, keywords in DOMAIN_KEYWORDS.items():
            scored[domain] = sum(
                1 for keyword in keywords if keyword in haystack)

        best = max(scored, key=scored.get)
        return best if scored[best] > 0 else DomainHint.GENERAL


def classify_domain(text: str) -> DomainHint:
    return create_domain_classifier("keyword").classify(text)


# Factory registry enables custom classifiers without changing triage callsites.
CLASSIFIER_REGISTRY: dict[str, Callable[[], DomainClassifier]] = {
    "keyword": KeywordDomainClassifier,
}


def register_domain_classifier(name: str, factory: Callable[[], DomainClassifier]) -> None:
    key = (name or "").strip().lower()
    if not key:
        raise ValueError("Classifier name must be non-empty")
    CLASSIFIER_REGISTRY[key] = factory


def create_domain_classifier(name: str = "keyword") -> DomainClassifier:
    key = (name or "keyword").strip().lower()
    factory = CLASSIFIER_REGISTRY.get(key)
    if factory is None:
        factory = CLASSIFIER_REGISTRY["keyword"]
    return factory()
