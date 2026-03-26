from __future__ import annotations

import concurrent.futures
import re
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from src.models.pageindex import PageIndex, PageIndexSection
from src.utils.ledger import write_json

if TYPE_CHECKING:
    from src.services.model_gateway import ModelGateway

ENRICH_MAX_WORKERS = 4
SECTION_TEXT_CAP = 5000
DEFAULT_SUMMARY_CHARS = 200

KEY_ENTITIES_PATTERNS = [
    re.compile(r"FY\s*\d{4}", re.I),
    re.compile(r"\d{4}[-/]\d{2}[-/]?\d{0,2}"),
    re.compile(r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}", re.I),
    re.compile(r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}", re.I),
    re.compile(r"\b(19|20)\d{2}\b"),
    re.compile(r"[\d,]+\s*(?:billion|million|trillion|USD|Birr|ETB)\b", re.I),
    re.compile(r"Ministry of [^.,;]+", re.I),
    re.compile(r"(?:Commercial\s+)?Bank\s+of\s+[A-Za-z]+", re.I),
    re.compile(r"\b(?:Information\s+)?(?:Systems|Network)\s+(?:Security|Division|DIVE)?\b", re.I),
    re.compile(r"Chapter\s+\d+", re.I),
    re.compile(r"Section\s+\d+(?:\.\d+)*", re.I),
    re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:\s+(?:Authority|Commission|Agency|Board|Committee|Report))\b"),
    re.compile(r"(?:Vulnerability\s+Disclosure|Standard\s+Procedure)\s*(?:\d{4})?", re.I),
    re.compile(r"\b(?:CBE|INSA|API|SSL|SQL|DoS|DDoS|HTTP|CSRF|XSS|HTML|CLI)\b"),
]


MAX_HEADING_LEVEL = 6
MIN_HEADING_LEVEL = 1


def _heading_level(title: str) -> int:
    """Return 1-based heading level. Clamped to [MIN_HEADING_LEVEL, MAX_HEADING_LEVEL]. Safeguards malformed titles."""
    t = (title or "").strip()
    if not t or len(t) > 500:
        return MIN_HEADING_LEVEL
    m = re.match(r"^(\d+(?:\.\d+)*)\.?", t)
    if m:
        level = len(m.group(1).split("."))
        return max(MIN_HEADING_LEVEL, min(MAX_HEADING_LEVEL, level))
    if re.match(r"^\s*(?:chapter|part)\s+\d+", t, re.I):
        return 1
    if re.match(r"^\s*section\s+\d+", t, re.I):
        return 2
    return MIN_HEADING_LEVEL


def _normalize_heading_level(level: int, parent_level: int) -> int:
    """Clamp level so we never skip hierarchy levels (e.g. 4 -> 1 becomes 2 if parent is 1)."""
    level = max(MIN_HEADING_LEVEL, min(MAX_HEADING_LEVEL, level))
    return min(level, parent_level + 1)


def _default_summary(section_title: str, section_text: str) -> str:
    text = (section_text or "").strip()
    if not text:
        return (section_title or "")[:DEFAULT_SUMMARY_CHARS]
    return text[:DEFAULT_SUMMARY_CHARS].strip()


def _extract_key_entities(section_text: str, max_entities: int = 8) -> list[str]:
    text = re.sub(r"\s+", " ", (section_text or "").strip())
    seen: set[str] = set()
    out: list[str] = []
    for pat in KEY_ENTITIES_PATTERNS:
        for m in pat.finditer(text):
            s = m.group(0).strip()
            if s and s.lower() not in seen and len(out) < max_entities:
                seen.add(s.lower())
                out.append(s)
    return out


def _data_types_from_text(section_text: str) -> list[str]:
    t = (section_text or "").lower()
    types = []
    if "table" in t or "tabular" in t:
        types.append("tables")
    if "figure" in t or "chart" in t or "graph" in t:
        types.append("figures")
    if types or t.strip():
        types.append("narrative")
    return list(dict.fromkeys(types)) if types else ["narrative"]


def _build_hierarchy(flat_sections: list[PageIndexSection]) -> list[PageIndexSection]:
    if not flat_sections:
        return []
    stack: list[tuple[int, PageIndexSection]] = []
    root_list: list[PageIndexSection] = []
    for sec in flat_sections:
        raw_level = _heading_level(sec.title)
        parent_level = stack[-1][0] if stack else 0
        level = _normalize_heading_level(raw_level, parent_level)
        while stack and stack[-1][0] >= level:
            stack.pop()
        node = PageIndexSection(
            section_id=sec.section_id,
            title=(sec.title or "").strip() or "(unnamed)",
            page_start=max(1, sec.page_start),
            page_end=max(1, max(sec.page_start, sec.page_end)),
            summary=sec.summary,
            key_entities=sec.key_entities,
            data_types_present=sec.data_types_present,
            child_sections=[],
        )
        if not stack:
            root_list.append(node)
        else:
            stack[-1][1].child_sections.append(node)
        stack.append((level, node))
    return root_list


def _parse_enrichment_response(text: str) -> tuple[str, list[str], list[str]]:
    summary = ""
    key_entities: list[str] = []
    data_types: list[str] = []
    if not (text or text.strip()):
        return summary, key_entities, data_types
    raw = text.strip()
    parts = [p.strip() for p in re.split(r"\n\s*\n", raw) if p.strip()]
    for part in parts:
        lower = part.lower()
        if lower.startswith("summary:"):
            summary = part.split(":", 1)[-1].strip()[:2000]
        elif "key entities:" in lower or "key entity:" in lower:
            rest = re.sub(r"^.*?key entit(?:y|ies):\s*", "", part, flags=re.I).strip()
            key_entities = [x.strip() for x in re.split(r"[,;]", rest) if x.strip()][:30]
        elif "data types:" in lower or "data type:" in lower:
            rest = re.sub(r"^.*?data type[s]?:\s*", "", part, flags=re.I).strip()
            data_types = [x.strip() for x in re.split(r"[,;]", rest) if x.strip()][:20]
    if not summary and parts:
        first = parts[0]
        if first.lower().startswith("key entities:") or first.lower().startswith("data types:"):
            pass
        else:
            summary = first[:2000].strip()
    return summary, key_entities, data_types


def _enrich_section(
    section: PageIndexSection,
    section_text: str,
    generate_fn: Callable[[str], str],
) -> None:
    excerpt = (section_text or "")[:800].strip()
    prompt = f"""Section title: {section.title}

Content excerpt:
{excerpt or '(no content)'}

Provide your response in this exact format (use these labels):
Summary: 2-3 sentences summarizing this section.
Key entities: comma-separated list (names, numbers, key terms).
Data types: comma-separated (e.g. tables, figures, narrative)."""
    try:
        response = generate_fn(prompt)
        summary, key_entities, data_types = _parse_enrichment_response(response)
        if summary:
            section.summary = summary
        if key_entities:
            section.key_entities = key_entities
        if data_types:
            section.data_types_present = data_types
    except Exception:
        pass


def enrich_pageindex(
    index: PageIndex,
    section_texts: dict[str, str],
    model_gateway: ModelGateway,
    doc_id: str,
    override: dict | None = None,
    max_sections_to_enrich: int | None = None,
    max_workers: int | None = None,
) -> PageIndex:
    """Enrich section summaries, key_entities, and data_types_present via LLM.
    Uses the default text model from model_gateway (model_selection.default_provider/model),
    e.g. Ollama llama3.1:8b, not the vision model. Override via config for a different model.
    max_sections_to_enrich caps how many sections get LLM enrichment to reduce indexing time.
    max_workers controls parallel section enrichment (default 4)."""
    from src.models import ModelProvider

    decision = model_gateway.select_model(
        query="summarize section",
        override=override,
        doc_id=doc_id,
    )
    adapter = model_gateway.providers.get(ModelProvider(decision.provider))
    if not adapter or not decision.model_name:
        return index

    def generate(prompt: str) -> str:
        result = adapter.generate(model_name=decision.model_name, prompt=prompt)
        return (result.text or "").strip()

    sections_to_enrich = [s for s in index._all_sections(index.root) if s.section_id != index.root.section_id]
    if max_sections_to_enrich is not None and max_sections_to_enrich > 0:
        sections_to_enrich = sections_to_enrich[:max_sections_to_enrich]
    workers = max(1, int(max_workers or ENRICH_MAX_WORKERS))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(_enrich_section, section, section_texts.get(section.section_id, ""), generate)
            for section in sections_to_enrich
        ]
        for fut in concurrent.futures.as_completed(futures):
            fut.result()

    all_text = " ".join(section_texts.get(s.section_id, "") for s in index._all_sections(index.root) if s.section_id != index.root.section_id)
    if all_text.strip():
        excerpt = all_text[:1500].strip()
        try:
            root_prompt = f"Document title: {index.root.title}\n\nContent excerpt:\n{excerpt}\n\nProvide in this format:\nSummary: 1-2 sentences describing what this document is about.\nKey entities: comma-separated list of main entities (e.g. organizations, dates, amounts).\nData types: comma-separated (e.g. tables, figures, narrative)."
            response = generate(root_prompt)
            summary, key_entities, data_types = _parse_enrichment_response(response)
            if summary:
                index.root.summary = summary
            if key_entities:
                index.root.key_entities = key_entities
            if data_types:
                index.root.data_types_present = data_types
        except Exception:
            pass
    return index


def build_pageindex(doc_id: str, document_name: str, pages: list[int], headings: list[str] | None = None) -> PageIndex:
    headings = headings or [f"Section {idx + 1}" for idx in range(len(pages) or 1)]
    child_sections: list[PageIndexSection] = []
    for idx, heading in enumerate(headings):
        page_num = pages[idx] if idx < len(pages) else pages[-1] if pages else 1
        child_sections.append(
            PageIndexSection(
                section_id=f"sec-{idx + 1}",
                title=heading,
                page_start=page_num,
                page_end=page_num,
                summary=f"Summary for {heading}",
                key_entities=[],
                data_types_present=[],
            )
        )

    root = PageIndexSection(
        section_id=f"root-{doc_id}",
        title=document_name,
        page_start=min(pages) if pages else 1,
        page_end=max(pages) if pages else 1,
        summary=f"Root section for {document_name}",
        child_sections=child_sections,
    )
    return PageIndex(doc_id=doc_id, root=root)


def build_pageindex_from_ldus(
    doc_id: str,
    document_name: str,
    chunks: list[dict],
    total_pages: int | None = None,
) -> PageIndex:
    """Build PageIndex from LDUs/chunks grouped by parent_section. No LLM calls; summary = first 200 chars, key_entities from regex.
    When total_pages is set, root always spans 1..total_pages so the index reflects the full document (100% extraction)."""
    by_section: dict[str, list[dict]] = {}
    for ch in chunks:
        sec = (ch.get("parent_section") or "").strip() or "(no section)"
        by_section.setdefault(sec, []).append(ch)

    flat: list[PageIndexSection] = []
    section_order = sorted(
        by_section.items(),
        key=lambda x: min((p for u in x[1] for p in (u.get("page_refs") or [1]))),
    )
    for idx, (title, ldus) in enumerate(section_order):
        page_refs = [p for u in ldus for p in (u.get("page_refs") or [1])]
        page_start = min(page_refs) if page_refs else 1
        page_end = max(page_refs) if page_refs else 1
        section_text = " ".join(str(u.get("text") or "").strip() for u in ldus)[:SECTION_TEXT_CAP].strip()
        summary = _default_summary(title, section_text)
        key_entities = _extract_key_entities(section_text)
        data_types = _data_types_from_text(section_text)
        section_id = f"sec-{idx + 1}"
        flat.append(
            PageIndexSection(
                section_id=section_id,
                title=title,
                page_start=page_start,
                page_end=page_end,
                summary=summary,
                key_entities=key_entities,
                data_types_present=data_types,
                child_sections=[],
            )
        )
    flat.sort(key=lambda s: (s.page_start, s.page_end))
    for i, sec in enumerate(flat):
        sec.section_id = f"sec-{i + 1}"
    root_children = _build_hierarchy(flat)
    all_pages = [p for s in flat for p in range(s.page_start, s.page_end + 1)]
    root_start = 1
    root_end = max(all_pages) if all_pages else 1
    if total_pages is not None and total_pages > 0:
        root_end = max(root_end, total_pages)
    root = PageIndexSection(
        section_id=f"root-{doc_id}",
        title=document_name,
        page_start=root_start,
        page_end=root_end,
        summary=_default_summary(document_name, " ".join(s.summary for s in flat)[:SECTION_TEXT_CAP]),
        key_entities=_extract_key_entities(" ".join(s.summary for s in flat), max_entities=12),
        data_types_present=["narrative"],
        child_sections=root_children,
    )
    return PageIndex(doc_id=doc_id, root=root)


def persist_pageindex(index: PageIndex, path: str | Path) -> None:
    write_json(path, index.model_dump())


def section_texts_from_ldus(index: PageIndex, chunks: list[dict]) -> dict[str, str]:
    section_texts: dict[str, str] = {}
    for section in index._all_sections(index.root):
        if section.section_id == index.root.section_id:
            continue
        page_range = set(range(section.page_start, section.page_end + 1))
        texts = []
        for ch in chunks:
            refs = ch.get("page_refs") or []
            if page_range.intersection(set(refs)):
                texts.append(str(ch.get("text") or ""))
        section_texts[section.section_id] = " ".join(texts)
    return section_texts
