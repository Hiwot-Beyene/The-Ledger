# Feature Specification: Document Intelligence Refinery MVP

**Feature ID**: `001-document-intelligence-refinery-mvp`  
**Created**: 2026-03-04  
**Status**: Draft  
**Input**: `docs/TRP1 Challenge Week 3_ The Document Intelligence Refinery.md`

## Scope

This MVP defines a production-grade specification for a five-stage, classification-aware document intelligence pipeline that converts heterogeneous documents into structured, queryable, provenance-preserving outputs.

In-scope:
- Document triage and profiling
- Multi-strategy extraction with confidence-gated escalation
- Semantic chunking into Logical Document Units (LDUs)
- PageIndex hierarchical navigation tree
- Query interface with provenance and audit mode
- Full-stack delivery: backend API + frontend application
- Dynamic model routing across Ollama and OpenRouter
- LangGraph orchestration and LangSmith tracing
- Async ingestion/extraction processing and status tracking
- Required artifacts and proof-of-execution outputs

Out-of-scope for this MVP spec:
- Managed cloud deployment and autoscaling infrastructure
- Multi-tenant auth/SSO and enterprise IAM

## Research-Backed Architecture Constraints

This specification follows documented production patterns for document intelligence systems and the challenge source document in `docs/TRP1 Challenge Week 3_ The Document Intelligence Refinery.md`.

The implementation MUST maintain a stage-by-stage architecture where each stage has explicit purpose, tooling options, and fallback alternatives.

### Stage Tooling Matrix (Documented Practices)

1. Stage 1 - Ingestion and Triage
- Purpose: classify origin/layout/language/domain and decide initial extraction strategy.
- Industry tools: `pdfplumber`, `PyMuPDF`, classifier heuristics, optional lightweight layout models.
- Free/open-source alternatives: `pdfplumber`, `pypdf`, `PyMuPDF`, keyword-based domain classification.

2. Stage 2 - Multi-Strategy Extraction (A/B/C)
- Purpose: extract structured text/tables/figures with confidence-gated escalation.
- Industry tools: fast parsers (`pdfplumber`, `PyMuPDF`), layout-aware parsers (`Docling`, `MinerU`, `Marker`), multimodal/VLM extraction for difficult pages.
- Free/open-source alternatives: `Docling`, `MinerU`, `Marker`, `PaddleOCR`, `Tesseract OCR`.

3. Stage 3 - Semantic Chunking (LDU generation)
- Purpose: convert normalized extraction into semantically coherent chunks suitable for retrieval.
- Industry tools: semantic splitters and chunk trees (e.g., LlamaIndex semantic chunking patterns), custom chunk validators.
- Free/open-source alternatives: LlamaIndex splitters, rule-based chunk engines with Pydantic validation.

4. Stage 4 - PageIndex and Navigation Layer
- Purpose: create hierarchical section navigation to reduce search space before retrieval.
- Industry tools: section hierarchy builders and metadata summarization; PageIndex-like structures.
- Free/open-source alternatives: custom tree builders over heading/numbering cues with local summarization models.

5. Stage 5 - Query and Verification Layer
- Purpose: orchestrate section navigation + semantic retrieval + structured SQL query with provenance-grounded answers.
- Industry tools: LangGraph-style orchestration, vector stores (ChromaDB/FAISS), SQLite fact querying.
- Free/open-source alternatives: LangGraph, ChromaDB, FAISS, SQLite.

### Multilingual and OCR Organization Requirements

- System MUST run language detection during triage and optionally per-page for mixed-language documents.
- OCR/vision path selection MUST consider language confidence and script characteristics.
- OCR output normalization MUST preserve Unicode text, spatial coordinates, and confidence metadata where available.
- For uncertain OCR/language cases, system MUST mark extraction confidence accordingly and escalate strategy rather than silently accepting weak text.

### LangGraph and LangSmith Placement

- LangGraph is REQUIRED in Stage 5 (query interface orchestration) and OPTIONAL in earlier stages.
- LangGraph SHOULD NOT be required for deterministic low-level extraction logic in Stage 1-2.
- LangSmith tracing is REQUIRED for query-time orchestration and model/tool decision visibility.
- LangSmith tracing for Stage 1-4 is OPTIONAL and implementation-dependent.

### Local-First and Cost Constraints

- The system MUST be operable without paid cloud storage dependencies.
- Local-first storage MUST use filesystem artifacts, local vector store, and SQLite.
- Model execution SHOULD prioritize local Ollama where quality requirements allow.
- OpenRouter usage MUST remain budget-guarded and configurable per document/query.
- Equivalent open-source alternatives MUST be documented for every non-local dependency.

## User Scenarios & Testing

### User Story 1 - Classify and Route Documents (Priority: P1)

As an FDE, I want each incoming document automatically profiled and routed to the correct extraction strategy so that extraction quality and cost are balanced per document type.

**Why this priority**: Correct triage is the control point for the entire pipeline; bad routing causes downstream extraction and RAG quality failures.

**Independent Test**: Run triage on representative documents from all 4 classes and verify `DocumentProfile` fields and selected strategy match expected class behavior.

**Acceptance Scenarios**:
1. **Given** a native digital single-column PDF with rich character stream, **When** triage runs, **Then** origin is classified as `native_digital` and strategy recommendation is `fast_text_sufficient`.
2. **Given** an image-based scanned PDF, **When** triage runs, **Then** origin is classified as `scanned_image` and strategy recommendation escalates to vision-capable extraction.
3. **Given** a mixed-layout, table-heavy PDF, **When** triage runs, **Then** layout is classified as `table_heavy` or `mixed` and layout-aware extraction is selected.

---

### User Story 2 - Extract Structured Content with Escalation Guard (Priority: P1)

As a data engineering stakeholder, I want extraction outputs normalized across strategies and automatically escalated on low confidence so that bad extraction does not contaminate downstream retrieval.

**Why this priority**: Extraction fidelity and confidence-gated retries are core reliability requirements and directly drive trust in the output.

**Independent Test**: Execute extraction on mixed corpus documents and verify strategy routing, confidence score behavior, retries/escalations, and ledger logging per document.

**Acceptance Scenarios**:
1. **Given** Strategy A is selected but page confidence falls below threshold, **When** extraction is evaluated, **Then** the system retries with Strategy B automatically.
2. **Given** a scanned or handwriting-heavy page, **When** extraction confidence from non-vision strategy is low, **Then** Strategy C is invoked within budget constraints.
3. **Given** any extraction run, **When** processing completes, **Then** `.refinery/extraction_ledger.jsonl` records strategy used, confidence score, cost estimate, and processing time.

---

### User Story 3 - Produce RAG-Ready Chunks and PageIndex (Priority: P2)

As an LLM application engineer, I want semantically coherent LDUs and a navigable PageIndex tree so that retrieval focuses on relevant sections instead of brute-force searching all chunks.

**Why this priority**: This is required to solve context poverty and improve retrieval precision in long enterprise documents.

**Independent Test**: Build LDUs and PageIndex for target documents; verify chunking constitution rules and top-3 section pre-navigation for topic queries.

**Acceptance Scenarios**:
1. **Given** extracted tables, figures, lists, and sections, **When** chunking runs, **Then** all chunking constitution rules are validated before chunk emission.
2. **Given** a user topic query, **When** PageIndex navigation runs first, **Then** it returns top relevant sections prior to semantic retrieval.
3. **Given** generated LDUs, **When** they are ingested into vector store, **Then** each LDU includes `content_hash`, `chunk_type`, `page_refs`, and `parent_section`.

---

### User Story 4 - Answer Questions with Full Provenance and Auditability (Priority: P2)

As a business analyst or auditor, I want each answer linked to exact source locations (page and bbox) and verifiable claims so outputs can be trusted and audited.

**Why this priority**: Provenance blindness is a critical enterprise blocker; source-linked evidence is mandatory for adoption.

**Independent Test**: Run natural-language queries and audit-mode verification; confirm all returned claims include `ProvenanceChain` and unresolved claims are marked unverifiable.

**Acceptance Scenarios**:
1. **Given** a natural-language question, **When** query agent answers, **Then** response includes document name, page number, bounding box, and content hash for each cited source.
2. **Given** a factual claim for verification, **When** audit mode runs, **Then** it returns either verified citations or `not found/unverifiable`.
3. **Given** financial numeric content, **When** fact table extraction runs, **Then** key numeric facts are queryable in SQLite with provenance linkage.

---

### User Story 5 - Operate Through a Responsive Full-Stack UI (Priority: P1)

As an analyst, I want to upload, manage, and query documents from a responsive web UI so that I can use the refinery without running command-line tools.

**Why this priority**: The challenge output must be demonstrable and usable end-to-end, including query with provenance.

**Independent Test**: Launch backend and frontend locally, upload documents, run processing, ask questions, and verify provenance in the UI.

**Acceptance Scenarios**:
1. **Given** the user opens the web app, **When** the layout renders, **Then** the left panel shows uploader, document list, and model configuration controls.
2. **Given** one or more documents are processed, **When** the user asks a question in chat, **Then** the right panel shows answer content and provenance citations (page + bbox + content hash).
3. **Given** mobile viewport constraints, **When** the app is loaded, **Then** layout remains usable with no blocked core actions.

---

### User Story 6 - Dynamic Model Selection and Observability (Priority: P1)

As an AI engineer, I want the system to recommend the best model per document/query while allowing user override and full trace visibility.

**Why this priority**: Cost/quality routing and traceability are core production requirements.

**Independent Test**: Run queries with auto-select on and off; verify selected provider/model, override behavior, and LangSmith trace records.

**Acceptance Scenarios**:
1. **Given** auto-select mode is enabled, **When** a query runs, **Then** the system selects a provider/model based on document profile, query intent, and budget policy.
2. **Given** user override is configured from UI, **When** a query runs, **Then** override provider/model is used and recorded in the ledger/trace metadata.
3. **Given** LangGraph agent tools execute, **When** tracing is enabled, **Then** LangSmith captures tool call sequence, selected model, and provenance metadata.

## Edge Cases

- Mixed document where only some pages are scanned and others are native digital.
- Table split across page boundaries with repeated header rows.
- Two-column legal text with footnotes and cross-references.
- Figure referenced before caption appears.
- Very low character density but high vector graphics (not truly scanned).
- Handwritten annotation overlays on otherwise digital pages.
- Budget cap reached before all pages can be processed by Strategy C.
- Language detection confidence is low or multiple languages are present.
- User forces a low-capability model override for a high-complexity query.
- OpenRouter provider outage or local Ollama model unavailable.
- Async extraction still running while the user sends query requests.

## Requirements

### Functional Requirements

- **FR-001**: System MUST profile each input document into a typed `DocumentProfile` before extraction.
- **FR-002**: System MUST classify origin type (`native_digital`, `scanned_image`, `mixed`, `form_fillable`) using measurable signals (character density, image area ratio, metadata).
- **FR-003**: System MUST classify layout complexity (`single_column`, `multi_column`, `table_heavy`, `figure_heavy`, `mixed`).
- **FR-004**: System MUST assign domain hint (`financial`, `legal`, `technical`, `medical`, `general`) via pluggable classifier.
- **FR-005**: System MUST estimate extraction cost tier (`fast_text_sufficient`, `needs_layout_model`, `needs_vision_model`).
- **FR-006**: System MUST persist profile output to `.refinery/profiles/{doc_id}.json`.
- **FR-007**: System MUST provide three extraction strategies (Fast Text, Layout-Aware, Vision-Augmented) behind a shared strategy interface.
- **FR-008**: System MUST implement confidence scoring for extraction quality using multi-signal page and document metrics.
- **FR-009**: System MUST enforce escalation guard: low confidence in Strategy A/B triggers higher-tier strategy automatically.
- **FR-010**: System MUST normalize all extraction outputs into a unified `ExtractedDocument` schema.
- **FR-011**: System MUST emit structured table data (headers + rows), text blocks with bbox, figures with captions, and reading order.
- **FR-012**: System MUST create extraction ledger entries in `.refinery/extraction_ledger.jsonl` for every processed document.
- **FR-013**: System MUST chunk extracted content into LDUs that enforce all five chunking constitution rules.
- **FR-014**: System MUST include provenance fields on each LDU (`page_refs`, `bounding_box`, `content_hash`).
- **FR-015**: System MUST build a hierarchical `PageIndex` tree with section boundaries, summaries, entities, and data types present.
- **FR-016**: System MUST support topic-to-section pre-navigation (`top-3` sections) before semantic retrieval.
- **FR-017**: System MUST ingest LDUs into a local vector store (ChromaDB or FAISS).
- **FR-018**: System MUST provide query tools: `pageindex_navigate`, `semantic_search`, `structured_query`.
- **FR-019**: System MUST return a `ProvenanceChain` for every answer with document, page, bbox, and content hash.
- **FR-020**: System MUST provide Audit Mode claim verification with explicit `verified` or `unverifiable` result.
- **FR-021**: System MUST process all four required document classes in the challenge corpus.
- **FR-022**: System MUST externalize thresholds/rules in `rubric/extraction_rules.yaml` so onboarding new document types is configuration-led.
- **FR-023**: System MUST expose backend APIs for upload, process, status, query, model configuration, and ledger retrieval.
- **FR-024**: System MUST provide a Next.js + Tailwind frontend with left control panel and right chat/provenance panel.
- **FR-025**: System MUST support dynamic model selection across Ollama and OpenRouter.
- **FR-026**: System MUST recommend a default model/provider based on document profile and query intent.
- **FR-027**: System MUST allow user override of provider/model from frontend configuration controls.
- **FR-028**: System MUST orchestrate query flow via LangGraph with tools `pageindex_navigate`, `semantic_search`, and `structured_query`.
- **FR-029**: System MUST integrate LangSmith tracing for agent/tool execution metadata.
- **FR-030**: System MUST process ingestion/extraction asynchronously and expose progress/status for UI polling.
- **FR-031**: System MUST persist query-time decisions (model provider, selected model, override flag, tool path) in traceable artifacts.
- **FR-032**: System MUST implement a pluggable model gateway interface so additional providers/models can be added without core query-agent rewrites.
- **FR-033**: System MUST include a stage-level tooling matrix (purpose, primary tools, open-source alternatives) in specification artifacts.
- **FR-034**: System MUST define multilingual handling flow (detection, OCR/script handling, normalization, confidence propagation).
- **FR-035**: System MUST define explicit stage placement for LangGraph (required in query stage, optional elsewhere).
- **FR-036**: System MUST define explicit stage placement for LangSmith tracing (required for query orchestration decisions).
- **FR-037**: System MUST support local-first execution without paid cloud storage services.
- **FR-038**: System MUST document source-backed references for major tooling and architectural choices.

### Non-Functional Requirements

- **NFR-001**: Pipeline stages MUST be modular and independently testable.
- **NFR-002**: Processing decisions MUST be traceable via profile files and extraction ledger.
- **NFR-003**: Vision extraction MUST enforce configurable budget cap per document.
- **NFR-004**: System MUST degrade gracefully (fallback/escalation) on low-confidence extraction.
- **NFR-005**: UI interactions for upload, status refresh, and chat responses SHOULD remain responsive under async backend processing.
- **NFR-006**: Query orchestration and model-routing modules MUST be independently testable with provider/tool mocks.
- **NFR-007**: Model selection policies MUST be configuration-driven and versioned for reproducibility.
- **NFR-008**: Architecture claims and tool choices MUST be traceable to documented public references.

### Key Entities

- **DocumentProfile**: Classification artifact controlling strategy routing.
- **ExtractedDocument**: Normalized extraction representation independent of strategy origin.
- **TextBlock**: Ordered textual unit with bbox and page reference.
- **TableObject**: Structured table representation with headers, rows, source page, bbox.
- **FigureObject**: Figure metadata with caption, bbox, and references.
- **LDU**: Logical Document Unit for retrieval with structural metadata and hash.
- **PageIndexNode**: Hierarchical section node with summary and entity metadata.
- **ProvenanceChain**: Evidence bundle linking answers to exact source locations.
- **ExtractionLedgerEntry**: Per-document extraction trace (strategy, confidence, cost, timing).
- **FactRecord**: Structured numeric fact extracted for SQL querying.
- **ModelSelectionDecision**: Provider/model choice with rationale, policy score, and override metadata.
- **QueryTraceRecord**: Query-time trace envelope with tool sequence, model decision, and citation set.
- **DocumentJobStatus**: Async ingestion/extraction job state and progress indicators.

## Success Criteria

### Measurable Outcomes

- **SC-001**: At least 12 documents are profiled (minimum 3 per class) with saved `DocumentProfile` artifacts.
- **SC-002**: 100% of processed documents produce extraction ledger entries with strategy, confidence, cost estimate, and runtime.
- **SC-003**: Escalation guard triggers automatically on low-confidence cases and is demonstrated in at least one run per applicable class.
- **SC-004**: For selected demo document, at least one complex table is extracted as structured JSON with correct header/value alignment.
- **SC-005**: PageIndex traversal returns top-3 relevant sections before vector retrieval for section-scoped questions.
- **SC-006**: Every query answer in demo includes full provenance (document, page, bbox, content hash).
- **SC-007**: Audit Mode verifies or rejects claims with explicit, evidence-backed output.
- **SC-008**: New document-type onboarding can be achieved by editing `rubric/extraction_rules.yaml` without code changes.
- **SC-009**: Frontend supports upload, document management, and chat query workflows on desktop and mobile layouts.
- **SC-010**: At least one end-to-end query per document class is executed through LangGraph with LangSmith trace capture.
- **SC-011**: Auto model recommendation and user override behavior are both demonstrated and auditable.
- **SC-012**: Query responses expose page + bbox + content hash provenance in UI.
- **SC-013**: Specification includes source-backed stage matrix with industry and open-source tool alternatives.
- **SC-014**: End-to-end flow can run fully in local-first mode using filesystem + local DB/vector store.
