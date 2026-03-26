# Data Model: Document Intelligence Refinery MVP

## 1. DocumentProfile

Purpose: Triage output controlling extraction routing.

Fields:
- `doc_id: str`
- `document_name: str`
- `origin_type: native_digital | scanned_image | mixed | form_fillable`
- `layout_complexity: single_column | multi_column | table_heavy | figure_heavy | mixed`
- `language.code: str` (ISO code, e.g., `en`, `am`)
- `language.confidence: float` (0-1)
- `domain_hint: financial | legal | technical | medical | general`
- `estimated_extraction_cost: fast_text_sufficient | needs_layout_model | needs_vision_model`
- `triage_signals.avg_char_density: float`
- `triage_signals.avg_whitespace_ratio: float`
- `triage_signals.avg_image_area_ratio: float`
- `triage_signals.table_density: float`
- `triage_signals.figure_density: float`
- `selected_strategy: A | B | C`

Validation rules:
- language confidence in `[0,1]`
- area/density ratios in `[0,1]`
- required enum values only

## 2. ExtractedDocument

Purpose: Normalized extraction output across all strategies.

Fields:
- `doc_id: str`
- `document_name: str`
- `pages: list[ExtractedPage]`
- `metadata.source_strategy: A | B | C`
- `metadata.confidence_score: float`

`ExtractedPage`:
- `page_number: int`
- `width: float`
- `height: float`
- `text_blocks: list[TextBlock]`
- `tables: list[TableObject]`
- `figures: list[FigureObject]`

`TextBlock`:
- `id: str`
- `text: str`
- `bbox: [x0,y0,x1,y1]`
- `reading_order: int`

`TableObject`:
- `id: str`
- `title: str | null`
- `headers: list[str]`
- `rows: list[list[str]]`
- `bbox: [x0,y0,x1,y1]`

`FigureObject`:
- `id: str`
- `caption: str | null`
- `bbox: [x0,y0,x1,y1]`
- `references: list[str]`

Validation rules:
- bbox has exactly 4 numeric values with `x1 >= x0`, `y1 >= y0`
- table rows align with header length or are explicitly marked ragged in metadata

## 3. LDU

Purpose: Semantic retrieval unit with structural context.

Fields:
- `ldu_id: str`
- `content: str`
- `chunk_type: paragraph | table | figure | list | heading | mixed`
- `page_refs: list[int]`
- `bounding_box: [x0,y0,x1,y1] | null`
- `parent_section: str | null`
- `token_count: int`
- `content_hash: str`
- `relationships: list[ChunkRelation]`

`ChunkRelation`:
- `type: cross_reference | continuation | caption_of`
- `target_id: str`

Validation rules:
- `token_count >= 0`
- non-empty `content_hash`

## 4. PageIndex

Purpose: Hierarchical navigation tree for section-first retrieval.

Fields:
- `doc_id: str`
- `root: PageIndexNode`

`PageIndexNode`:
- `section_id: str`
- `title: str`
- `page_start: int`
- `page_end: int`
- `summary: str`
- `key_entities: list[str]`
- `data_types_present: list[tables|figures|equations|narrative]`
- `child_sections: list[PageIndexNode]`

Validation rules:
- `page_end >= page_start`

## 5. ProvenanceChain

Purpose: Verifiable citation trace for answers.

Fields:
- `answer_id: str`
- `citations: list[ProvenanceCitation]`
- `verification_status: verified | unverifiable | partial`

`ProvenanceCitation`:
- `document_name: str`
- `page_number: int`
- `bbox: [x0,y0,x1,y1]`
- `content_hash: str`
- `excerpt: str`

Validation rules:
- at least one citation when status is `verified`

## 6. ExtractionLedgerEntry

Purpose: Audit log of strategy decisions and cost.

Fields:
- `timestamp: str` (ISO8601)
- `doc_id: str`
- `document_name: str`
- `strategy_sequence: list[A|B|C]`
- `final_strategy: A|B|C`
- `confidence_score: float`
- `cost_estimate_usd: float`
- `processing_time_ms: int`
- `budget_cap_usd: float`
- `budget_status: under_cap | cap_reached`
- `notes: str | null`

Validation rules:
- non-negative `processing_time_ms` and `cost_estimate_usd`
- confidence in `[0,1]`

## 7. ModelSelectionDecision

Purpose: Captures model/provider routing decisions for extraction and query steps.

Fields:
- `decision_id: str`
- `doc_id: str | null`
- `query_id: str | null`
- `provider: ollama | openrouter`
- `model_name: str`
- `mode: auto | user_override`
- `reasoning: str`
- `estimated_cost_usd: float`
- `estimated_latency_ms: int`
- `timestamp: str` (ISO8601)

Validation rules:
- non-negative `estimated_cost_usd` and `estimated_latency_ms`
- `doc_id` or `query_id` must be present

## 8. DocumentJobStatus

Purpose: Tracks asynchronous processing lifecycle for UI and API consumers.

Fields:
- `job_id: str`
- `doc_id: str`
- `stage: triage | extraction | chunking | indexing | completed | failed`
- `status: queued | running | completed | failed`
- `progress_percent: int`
- `message: str | null`
- `started_at: str | null`
- `updated_at: str`

Validation rules:
- `progress_percent` in `[0,100]`
- `status=completed` requires `stage=completed`

## 9. QueryTraceRecord

Purpose: Normalized trace envelope for query executions.

Fields:
- `query_id: str`
- `doc_ids: list[str]`
- `tool_sequence: list[str]`
- `model_decision: ModelSelectionDecision`
- `citations: list[ProvenanceCitation]`
- `langsmith_trace_id: str | null`
- `created_at: str`

Validation rules:
- `tool_sequence` must be non-empty
- `citations` required for verified answers
