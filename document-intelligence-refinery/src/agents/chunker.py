"""
Semantic Chunking Engine: ExtractedDocument -> List[LDU] with constitution rules.
- Table cell never split from header (one LDU per table).
- Figure caption stored as metadata of figure chunk (caption in LDU text).
- Numbered list kept as single LDU unless exceeds max_tokens.
- Section headers stored as parent_section on child chunks.
- Cross-references resolved and stored as reference_ids.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from src.models.extracted_document import (
    BBox,
    ExtractedDocument,
    FigureObject,
    LDU,
    ProvenanceChain,
    TableObject,
    TextBlock,
    content_hash_for_text,
    estimate_token_count,
)

SENTENCE_END = re.compile(r"[.!?]\s*$")
MAX_MERGE_WORDS = 80
MIN_CHUNK_CHARS = 50
MAX_LIST_TOKENS = 512
MAX_TABLE_ROWS_PER_CHUNK = 10

HEADING_NUMBERED = re.compile(r"^\s*\d+(\.\d+)*\.?\s+\S")
HEADING_CHAPTER = re.compile(r"^\s*(?:chapter|section|part|annex)\s+\d+", re.I)
HEADING_SHORT_TITLE = re.compile(r"^[A-Z][a-zA-Z\s]{2,80}$")
NUMBERED_LIST_ITEM = re.compile(r"^\s*(?:\d+[.)]|\d+\.\d+[.)])\s+\S")
CROSS_REF_TABLE = re.compile(r"\b(?:table|tab\.?)\s*(\d+)\b", re.I)
CROSS_REF_FIGURE = re.compile(r"\b(?:figure|fig\.?)\s*(\d+)\b", re.I)
CROSS_REF_SECTION = re.compile(r"\b(?:section|sec\.?|§)\s*(\d+(?:\.\d+)*)\b", re.I)


def _is_heading(text: str) -> bool:
    t = (text or "").strip()
    if not t or len(t) > 200:
        return False
    if HEADING_NUMBERED.match(t) or HEADING_CHAPTER.match(t):
        return True
    if len(t) < 15 and HEADING_SHORT_TITLE.match(t) and not t.endswith("."):
        return True
    return False


def _is_numbered_list_item(text: str) -> bool:
    return bool(NUMBERED_LIST_ITEM.match((text or "").strip()))


def _table_to_markdown(t: TableObject) -> str:
    parts = []
    if t.headers:
        parts.append("| " + " | ".join(str(h) for h in t.headers) + " |")
        parts.append("| " + " | ".join("---" for _ in t.headers) + " |")
    for row in t.rows:
        parts.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(parts) if parts else (t.title or "Table")


def _table_label_and_values(headers: list[str], row: list[str]) -> tuple[str, list[str]]:
    """Pick label (first semantic column) and value columns so label and values stay unambiguous."""
    if not row:
        return "", []
    if len(headers) >= 3 and len(row) >= 3:
        c0, c1 = (row[0] or "").strip(), (row[1] or "").strip()
        mostly_numeric = re.match(r"^[\d,\s\.\(\)\-]+$", (c1 or ""))
        if c1 and not mostly_numeric and len(c1) > len(c0):
            return c1, row[2:]
    return (row[0] or "").strip(), row[1:]


def _table_to_semantic_content(
    t: TableObject, row_start: int, row_end: int
) -> str:
    """Format table section as row-semantic lines: 'label: val1 | val2' per row so retrieval matches label to values."""
    lines = []
    if t.title:
        lines.append(t.title)
    if t.headers:
        lines.append("Columns: " + " | ".join(str(h).strip() for h in t.headers))
    for r in t.rows[row_start:row_end]:
        label, vals = _table_label_and_values(t.headers, r)
        if label or vals:
            value_str = " | ".join(str(v).strip() for v in vals) if vals else ""
            lines.append(f"{label}: {value_str}".strip())
    return "\n".join(lines) if lines else (t.title or "Table")


def _provenance_chain(
    document_name: str, page_number: int, bbox: BBox, text: str
) -> list[ProvenanceChain]:
    return [
        ProvenanceChain(
            document_name=document_name,
            page_number=page_number,
            bbox=bbox,
            content_hash=content_hash_for_text(text),
        )
    ]


@dataclass
class _PageElement:
    page_number: int
    order_key: float
    kind: str
    payload: TextBlock | TableObject | FigureObject


def _ordered_elements(extracted: ExtractedDocument) -> list[_PageElement]:
    elements: list[_PageElement] = []
    for page in extracted.pages:
        ro = 0
        for tb in sorted(page.text_blocks, key=lambda b: (b.reading_order, b.bbox.y0)):
            elements.append(
                _PageElement(
                    page_number=page.page_number,
                    order_key=ro * 1000 + tb.bbox.y0,
                    kind="text_block",
                    payload=tb,
                )
            )
            ro += 1
        for ti, tbl in enumerate(sorted(page.tables, key=lambda t: (t.reading_order, t.bbox.y0))):
            elements.append(
                _PageElement(
                    page_number=page.page_number,
                    order_key=100000 + ti * 1000 + tbl.bbox.y0,
                    kind="table",
                    payload=tbl,
                )
            )
        for fi, fig in enumerate(sorted(page.figures, key=lambda f: (f.reading_order, f.bbox.y0))):
            elements.append(
                _PageElement(
                    page_number=page.page_number,
                    order_key=200000 + fi * 1000 + fig.bbox.y0,
                    kind="figure",
                    payload=fig,
                )
            )
    elements.sort(key=lambda e: (e.page_number, e.order_key))
    return elements


def _resolve_cross_refs(
    text: str,
    table_ids_by_index: dict[int, str],
    figure_ids_by_index: dict[int, str],
) -> list[str]:
    refs: list[str] = []
    for m in CROSS_REF_TABLE.finditer(text):
        idx = int(m.group(1))
        if idx in table_ids_by_index:
            refs.append(table_ids_by_index[idx])
    for m in CROSS_REF_FIGURE.finditer(text):
        idx = int(m.group(1))
        if idx in figure_ids_by_index:
            refs.append(figure_ids_by_index[idx])
    return list(dict.fromkeys(refs))


class ChunkingEngine:
    """Builds List[LDU] from ExtractedDocument enforcing the five chunking rules."""

    def __init__(
        self,
        max_list_tokens: int = MAX_LIST_TOKENS,
    ):
        self.max_list_tokens = max_list_tokens

    def build(self, extracted: ExtractedDocument) -> list[LDU]:
        if not extracted.pages:
            return self._enrich_existing_ldus(extracted)

        elements = _ordered_elements(extracted)
        ldus: list[LDU] = []
        current_section = "(no section)"
        table_index_to_id: dict[int, str] = {}
        figure_index_to_id: dict[int, str] = {}
        table_counter = 0
        figure_counter = 0
        ldu_counter = 0

        i = 0
        while i < len(elements):
            el = elements[i]
            # Per-page section when no heading (PageIndex gets one section per page for full-doc navigation).
            effective_section = (
                current_section
                if (current_section and current_section != "(no section)")
                else f"Page {el.page_number}"
            )
            if el.kind == "text_block":
                block = el.payload
                text = (block.text or "").strip()
                if not text:
                    i += 1
                    continue
                if _is_heading(text):
                    current_section = text
                    prov = _provenance_chain(
                        extracted.document_name, el.page_number, block.bbox, text
                    )
                    ldu = LDU(
                        id=f"ldu-{block.id}",
                        text=text,
                        content_hash=content_hash_for_text(text),
                        chunk_type="heading",
                        bounding_box=block.bbox,
                        token_count=estimate_token_count(text),
                        parent_section=current_section,
                        page_refs=[el.page_number],
                        provenance_chain=prov,
                    )
                    ldus.append(ldu)
                    ldu_counter += 1
                    i += 1
                    continue
                if _is_numbered_list_item(text):
                    list_texts = [text]
                    list_blocks = [block]
                    list_prov: list[ProvenanceChain] = []
                    list_prov.extend(
                        _provenance_chain(
                            extracted.document_name, el.page_number, block.bbox, text
                        )
                    )
                    j = i + 1
                    total_tokens = estimate_token_count(text)
                    while j < len(elements) and elements[j].kind == "text_block":
                        nb = elements[j].payload
                        nt = (nb.text or "").strip()
                        if not _is_numbered_list_item(nt):
                            break
                        total_tokens += estimate_token_count(nt)
                        if total_tokens > self.max_list_tokens:
                            break
                        list_texts.append(nt)
                        list_blocks.append(nb)
                        list_prov.extend(
                            _provenance_chain(
                                extracted.document_name,
                                elements[j].page_number,
                                nb.bbox,
                                nt,
                            )
                        )
                        j += 1
                    combined = "\n".join(list_texts)
                    first_b = list_blocks[0]
                    ldu = LDU(
                        id=f"ldu-{first_b.id}-list",
                        text=combined,
                        content_hash=content_hash_for_text(combined),
                        chunk_type="list",
                        bounding_box=first_b.bbox,
                        token_count=estimate_token_count(combined),
                        parent_section=effective_section,
                        page_refs=list(
                            dict.fromkeys(
                                elements[k].page_number for k in range(i, j)
                            )
                        ),
                        provenance_chain=list_prov,
                    )
                    ldus.append(ldu)
                    ldu_counter += 1
                    i = j
                    continue
                prov = _provenance_chain(
                    extracted.document_name, el.page_number, block.bbox, text
                )
                ldu = LDU(
                    id=f"ldu-{block.id}",
                    text=text,
                    content_hash=content_hash_for_text(text),
                    chunk_type="paragraph",
                    bounding_box=block.bbox,
                    token_count=estimate_token_count(text),
                    parent_section=effective_section,
                    page_refs=[el.page_number],
                    provenance_chain=prov,
                )
                ldus.append(ldu)
                ldu_counter += 1
                i += 1
            elif el.kind == "table":
                tbl = el.payload
                table_counter += 1
                table_index_to_id[table_counter] = f"ldu-{tbl.id}"
                num_rows = len(tbl.rows)
                if num_rows <= MAX_TABLE_ROWS_PER_CHUNK:
                    content = _table_to_semantic_content(tbl, 0, num_rows)
                    prov = _provenance_chain(
                        extracted.document_name, el.page_number, tbl.bbox, content
                    )
                    ldus.append(
                        LDU(
                            id=f"ldu-{tbl.id}",
                            text=content,
                            content_hash=content_hash_for_text(content),
                            chunk_type="table",
                            bounding_box=tbl.bbox,
                            token_count=estimate_token_count(content),
                            parent_section=effective_section,
                            page_refs=[el.page_number],
                            provenance_chain=prov,
                        )
                    )
                else:
                    for sec_start in range(0, num_rows, MAX_TABLE_ROWS_PER_CHUNK):
                        sec_end = min(sec_start + MAX_TABLE_ROWS_PER_CHUNK, num_rows)
                        content = _table_to_semantic_content(tbl, sec_start, sec_end)
                        prov = _provenance_chain(
                            extracted.document_name, el.page_number, tbl.bbox, content
                        )
                        sec_id = f"ldu-{tbl.id}-sec{sec_start}" if sec_start > 0 else f"ldu-{tbl.id}"
                        ldus.append(
                            LDU(
                                id=sec_id,
                                text=content,
                                content_hash=content_hash_for_text(content),
                                chunk_type="table",
                                bounding_box=tbl.bbox,
                                token_count=estimate_token_count(content),
                                parent_section=effective_section,
                                page_refs=[el.page_number],
                                provenance_chain=prov,
                            )
                        )
                i += 1
            else:
                fig = el.payload
                figure_counter += 1
                figure_index_to_id[figure_counter] = f"ldu-{fig.id}"
                content = (fig.caption or "").strip() or "Figure"
                prov = _provenance_chain(
                    extracted.document_name, el.page_number, fig.bbox, content
                )
                ldu = LDU(
                    id=f"ldu-{fig.id}",
                    text=content,
                    content_hash=content_hash_for_text(content),
                    chunk_type="figure",
                    bounding_box=fig.bbox,
                    token_count=estimate_token_count(content),
                    parent_section=effective_section,
                    page_refs=[el.page_number],
                    reference_ids=fig.references[:],
                    provenance_chain=prov,
                )
                ldus.append(ldu)
                i += 1

        for ldu in ldus:
            refs = _resolve_cross_refs(
                ldu.text, table_index_to_id, figure_index_to_id
            )
            if refs:
                ldu.reference_ids = list(dict.fromkeys((ldu.reference_ids or []) + refs))

        for idx, ldu in enumerate(ldus):
            if idx > 0:
                ldu.previous_chunk_id = ldus[idx - 1].id
            if idx + 1 < len(ldus):
                ldu.next_chunk_id = ldus[idx + 1].id

        assert_constitution(ldus, max_list_tokens=self.max_list_tokens)
        return ldus

    def _enrich_existing_ldus(self, extracted: ExtractedDocument) -> list[LDU]:
        if not extracted.ldus:
            return []
        result: list[LDU] = []
        for ldu in extracted.ldus:
            text = (ldu.text or "").strip()
            bbox = None
            if ldu.provenance_chain:
                bbox = ldu.provenance_chain[0].bbox
            result.append(
                LDU(
                    id=ldu.id,
                    text=text,
                    content_hash=ldu.content_hash,
                    chunk_type=getattr(ldu, "chunk_type", "paragraph") or "paragraph",
                    bounding_box=getattr(ldu, "bounding_box", None) or bbox,
                    token_count=getattr(ldu, "token_count", None)
                    or estimate_token_count(text),
                    parent_section=ldu.parent_section,
                    previous_chunk_id=ldu.previous_chunk_id,
                    next_chunk_id=ldu.next_chunk_id,
                    reference_ids=getattr(ldu, "reference_ids", []) or [],
                    page_refs=ldu.page_refs,
                    provenance_chain=ldu.provenance_chain,
                )
            )
        for idx, ldu in enumerate(result):
            if idx > 0:
                ldu.previous_chunk_id = result[idx - 1].id
            if idx + 1 < len(result):
                ldu.next_chunk_id = result[idx + 1].id
        return result


def build_ldus(extracted: ExtractedDocument) -> list[LDU]:
    """Build LDUs from ExtractedDocument. Uses ChunkingEngine when pages exist."""
    if extracted.pages:
        return ChunkingEngine().build(extracted)
    if extracted.ldus:
        return ChunkingEngine()._enrich_existing_ldus(extracted)
    return []


def _ldu_dict(ldu: LDU) -> dict:
    return ldu.model_dump() if hasattr(ldu, "model_dump") else ldu


def merge_ldus_for_ingestion(ldus: list[dict] | list[LDU]) -> list[dict]:
    if not ldus:
        return []
    raw = [_ldu_dict(l) if isinstance(l, LDU) else l for l in ldus]
    if len(raw) <= 1:
        return raw
    first_text = (raw[0].get("text") or "").strip()
    if len(first_text) >= MIN_CHUNK_CHARS and not re.search(r"^\S+$", first_text):
        return raw
    merged: list[dict] = []
    batch: list[dict] = []
    word_count = 0
    for i, ldu in enumerate(raw):
        text = (ldu.get("text") or "").strip()
        batch.append(ldu)
        word_count += len(text.split())
        at_sentence_end = SENTENCE_END.search(text)
        if at_sentence_end or word_count >= MAX_MERGE_WORDS or i == len(raw) - 1:
            combined = " ".join((b.get("text") or "").strip() for b in batch)
            if combined:
                first = batch[0]
                # Preserve full page range across merged batch for correct PageIndex (100% extraction).
                all_page_refs = sorted(set(p for b in batch for p in (b.get("page_refs") or [1])))
                merged.append({
                    "id": first.get("id", "") + f"-m{len(merged)}",
                    "text": combined,
                    "content_hash": content_hash_for_text(combined),
                    "page_refs": all_page_refs if all_page_refs else (first.get("page_refs") or [1]),
                    "parent_section": first.get("parent_section"),
                    "chunk_type": first.get("chunk_type") or "mixed",
                    "provenance_chain": first.get("provenance_chain") or [],
                })
            batch = []
            word_count = 0
    return merged if merged else raw


CONSTITUTION_RULES = (
    "table_cell_with_header",
    "figure_caption_attached",
    "list_unit_preserved",
    "parent_section_propagation",
    "cross_reference_links",
)


def _validate_rule_table_cell_with_header(ldu: LDU) -> list[str]:
    """Table LDU must include header context (Columns: or header row); table never split from header."""
    if (ldu.chunk_type or "") != "table":
        return []
    issues = []
    text = (ldu.text or "").strip()
    if not text:
        issues.append("table_chunk_empty")
    elif "Columns:" not in text and "|" not in text:
        issues.append("table_chunk_missing_header_context")
    return issues


def _validate_rule_figure_caption_attached(ldu: LDU) -> list[str]:
    """Figure chunk must carry caption (or placeholder) as text."""
    if (ldu.chunk_type or "") != "figure":
        return []
    if not (ldu.text or "").strip():
        return ["figure_chunk_missing_caption"]
    return []


def _validate_rule_list_unit_preserved(ldu: LDU, max_list_tokens: int = MAX_LIST_TOKENS) -> list[str]:
    """Numbered list LDU must not exceed max_list_tokens (list not fragmented)."""
    if (ldu.chunk_type or "") != "list":
        return []
    issues = []
    tc = ldu.token_count
    if tc is not None and tc > max_list_tokens:
        issues.append("list_chunk_exceeds_max_tokens")
    return issues


def _validate_rule_parent_section_propagation(ldu: LDU) -> list[str]:
    """Every chunk must have parent_section set."""
    if not (ldu.parent_section or "").strip():
        return ["missing_parent_section"]
    return []


def _validate_rule_cross_reference_links(ldu: LDU) -> list[str]:
    """reference_ids must be a list (cross-refs stored). No strict content check."""
    if not isinstance(getattr(ldu, "reference_ids", None), list):
        return ["invalid_reference_ids"]
    return []


def validate_chunk(ldu: LDU, max_list_tokens: int = MAX_LIST_TOKENS) -> list[str]:
    issues: list[str] = []
    if not (ldu.content_hash or "").strip():
        issues.append("missing_content_hash")
    if not (ldu.page_refs or []):
        issues.append("missing_page_refs")
    if not (ldu.provenance_chain or []):
        issues.append("missing_provenance")
    issues.extend(_validate_rule_parent_section_propagation(ldu))
    issues.extend(_validate_rule_table_cell_with_header(ldu))
    issues.extend(_validate_rule_figure_caption_attached(ldu))
    issues.extend(_validate_rule_list_unit_preserved(ldu, max_list_tokens))
    issues.extend(_validate_rule_cross_reference_links(ldu))
    return issues


def validate_ldus_constitution(
    ldus: list[LDU],
    max_list_tokens: int = MAX_LIST_TOKENS,
) -> list[tuple[str, str, str]]:
    """Validate all five constitution rules across LDUs. Returns (ldu_id, rule, issue)."""
    violations: list[tuple[str, str, str]] = []
    for ldu in ldus or []:
        lid = getattr(ldu, "id", "") or ""
        for issue in validate_chunk(ldu, max_list_tokens=max_list_tokens):
            rule = "parent_section_propagation" if issue == "missing_parent_section" else (
                "table_cell_with_header" if issue.startswith("table_") else (
                    "figure_caption_attached" if issue.startswith("figure_") else (
                        "list_unit_preserved" if issue.startswith("list_") else (
                            "cross_reference_links" if "reference" in issue else "unknown"
                        )
                    )
                )
            )
            violations.append((lid, rule, issue))
    return violations


def assert_constitution(
    ldus: list[LDU],
    max_list_tokens: int = MAX_LIST_TOKENS,
) -> None:
    """Raise AssertionError if any LDU violates the five constitution rules."""
    violations = validate_ldus_constitution(ldus, max_list_tokens=max_list_tokens)
    if not violations:
        return
    lines = [f"  {lid}: [{rule}] {msg}" for lid, rule, msg in violations]
    raise AssertionError("Chunking constitution violated:\n" + "\n".join(lines))
