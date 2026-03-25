"""
Natural-language routing for staff Navigator: extract decision-history intent and application id.

Flexible phrasing via synonym families and regex templates — not a single canned sentence.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

_HISTORY_HINTS: frozenset[str] = frozenset(
    {
        "history",
        "historical",
        "audit",
        "auditable",
        "trail",
        "stream",
        "narrative",
        "package",
        "examination",
        "regulatory",
        "ledger",
        "navigator",
        "timeline",
        "immutable",
        "decision",
        "outcome",
        "verdict",
        "cryptographic",
        "integrity",
        "causal",
        "compliance",
        "agent",
        "review",
        "human",
    }
)

_APPLICATION_HINTS: frozenset[str] = frozenset(
    {
        "application",
        "applicant",
        "app",
        "loan",
        "case",
        "file",
        "dossier",
    }
)

_REQUEST_VERBS: frozenset[str] = frozenset(
    {
        "show",
        "display",
        "pull",
        "get",
        "fetch",
        "load",
        "give",
        "see",
        "view",
        "want",
        "need",
        "outline",
        "walk",
        "export",
        "print",
        "run",
        "open",
    }
)

_COMPLETENESS: frozenset[str] = frozenset(
    {
        "complete",
        "full",
        "entire",
        "whole",
        "everything",
        "e2e",
    }
)

_STOP_ID_TOKENS: frozenset[str] = frozenset(
    {
        "me",
        "my",
        "the",
        "a",
        "an",
        "to",
        "for",
        "of",
        "is",
        "be",
        "and",
        "or",
        "in",
        "on",
        "at",
        "as",
        "by",
        "it",
        "id",
        "no",
        "not",
        "all",
        "any",
        "can",
        "you",
        "please",
        "would",
        "could",
        "show",
        "get",
        "give",
        "see",
        "view",
        "want",
        "need",
        "run",
        "open",
        "pull",
        "load",
        "fetch",
        "display",
        "tell",
    }
)

_ID_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")

_STRUCT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"(?:application|app)\s*(?:id|identifier|number|#|no\.?)\s*[:#]?\s*['\"]?([A-Za-z0-9][A-Za-z0-9_.-]*)['\"]?",
        re.I,
    ),
    re.compile(
        r"(?:for|on)\s+(?:the\s+)?(?:application|app|loan|case)\s+(?:id|identifier|number|#|no\.?)\s*[:#]?\s*"
        r"['\"]?([A-Za-z0-9][A-Za-z0-9_.-]*)['\"]?",
        re.I,
    ),
    re.compile(r"\bloan\s*[-:/]\s*['\"]?([A-Za-z0-9][A-Za-z0-9_.-]*)['\"]?", re.I),
    re.compile(r"(?:application|app)\s+['\"]([A-Za-z0-9][A-Za-z0-9_.-]*)['\"]", re.I),
)

_STRUCT_ID_STOPWORDS: frozenset[str] = frozenset(
    {"id", "no", "for", "the", "date", "time", "history", "audit", "trail", "stream", "case"},
)


def _structured_capture_ok(token: str) -> bool:
    low = token.lower()
    return low not in _STRUCT_ID_STOPWORDS and low not in _STOP_ID_TOKENS

_ISO_RE = re.compile(
    r"\b(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:?\d{2})?)\b"
)


def _normalize_application_id(raw: str) -> str:
    s = raw.strip().strip("'\"")
    low = s.lower()
    for prefix in ("loan-", "loan:", "stream:"):
        if low.startswith(prefix):
            s = s[len(prefix) :]
            break
    return s.strip().strip("'\"")


def _parse_iso(s: str) -> datetime | None:
    t = s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _extract_examination_at(text: str) -> tuple[datetime | None, str | None]:
    m_asof = re.search(r"as\s+of\s+(\d{4}-\d{2}-\d{2}T[^\s,;.]+)", text, re.I)
    if m_asof:
        raw = m_asof.group(1).rstrip(").,;")
        dt = _parse_iso(raw)
        if dt:
            return dt, "as_of_phrase"
    for m in _ISO_RE.finditer(text):
        dt = _parse_iso(m.group(1))
        if dt:
            return dt, "iso_token"
    return None, None


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+(?:-[a-z0-9]+)*", text.lower())


def _intent_score(normalized_text: str, toks: Iterable[str]) -> tuple[int, bool]:
    tset = set(toks)
    score = 0
    if "end-to-end" in normalized_text or "endtoend" in normalized_text:
        score += 4
    if _HISTORY_HINTS & tset:
        score += 3
    if _APPLICATION_HINTS & tset:
        score += 2
    if _REQUEST_VERBS & tset:
        score += 1
    if _COMPLETENESS & tset:
        score += 2
    multi = "decision history" in normalized_text or "audit trail" in normalized_text
    return score, multi


def _structured_ids(text: str) -> tuple[list[str], list[str]]:
    found: list[str] = []
    signals: list[str] = []
    for i, pat in enumerate(_STRUCT_PATTERNS):
        for m in pat.finditer(text):
            aid = _normalize_application_id(m.group(1))
            if aid and _structured_capture_ok(aid):
                found.append(aid)
                signals.append(f"structured_pattern_{i + 1}:{aid}")
    return found, signals


def _heuristic_candidates(text: str, normalized: str) -> tuple[list[str], list[str]]:
    candidates: dict[str, int] = {}
    signals: list[str] = []
    lowered = text.lower()
    for m in _ID_TOKEN_RE.finditer(text):
        raw = m.group(0)
        low = raw.lower()
        if low in _STOP_ID_TOKENS:
            continue
        if low.isdigit():
            continue
        aid = _normalize_application_id(raw)
        if not aid:
            continue
        pos = m.start()
        window = lowered[max(0, pos - 48) : pos + 48]
        score = 1
        if any(h in window for h in _APPLICATION_HINTS):
            score += 4
        if any(h in window for h in _HISTORY_HINTS):
            score += 3
        candidates[aid] = candidates.get(aid, 0) + score
    if not candidates:
        return [], signals
    best = max(candidates.values())
    top = sorted([k for k, v in candidates.items() if v == best], key=len, reverse=True)
    signals.append(f"heuristic_scores:{candidates!s}")
    return top, signals


@dataclass
class NavigatorParseOutcome:
    application_id: str | None = None
    examination_at: datetime | None = None
    intent_matched: bool = False
    signals: list[str] = field(default_factory=list)
    error: str | None = None


def parse_navigator_decision_history_query(text: str) -> NavigatorParseOutcome:
    raw = (text or "").strip()
    if len(raw) < 5:
        return NavigatorParseOutcome(error="Ask in a short sentence (e.g. complete decision history for an application).")

    normalized = " ".join(raw.lower().split())
    toks = _tokens(normalized)
    score, multi_phrase = _intent_score(normalized, toks)

    struct_ids, struct_signals = _structured_ids(raw)
    struct_ids = [_normalize_application_id(x) for x in struct_ids if x]
    struct_unique = list(dict.fromkeys(struct_ids))

    exam_at, exam_sig = _extract_examination_at(raw)
    signals: list[str] = list(struct_signals)
    if exam_sig:
        signals.append(f"examination_at:{exam_sig}")

    if len(struct_unique) > 1:
        return NavigatorParseOutcome(
            signals=signals,
            error=f"Multiple application identifiers found ({', '.join(struct_unique)}). Name one application.",
        )

    app_id: str | None = struct_unique[0] if struct_unique else None

    structured_ok = bool(app_id)
    intent_ok = structured_ok or score >= 6 or (multi_phrase and score >= 4) or (score >= 5)

    if not app_id:
        cands, h_sig = _heuristic_candidates(raw, normalized)
        signals.extend(h_sig)
        if not intent_ok and cands:
            return NavigatorParseOutcome(
                signals=signals,
                error="That does not look like a request for decision history / audit trail. Mention history, audit, full stream, or similar.",
            )
        if intent_ok and len(cands) == 1:
            app_id = cands[0]
            signals.append(f"heuristic_pick:{app_id}")
        elif intent_ok and len(cands) > 1:
            return NavigatorParseOutcome(
                signals=signals,
                error=f"Which application? Candidates: {', '.join(cands[:8])}. Name the id clearly (e.g. application ID …).",
            )

    if not app_id:
        return NavigatorParseOutcome(
            signals=signals,
            error="No application id found. Include it explicitly (e.g. “application ID APEX-0001”).",
        )

    return NavigatorParseOutcome(
        application_id=app_id,
        examination_at=exam_at,
        intent_matched=True,
        signals=signals,
    )
