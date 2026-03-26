# Tasks: Document Intelligence Refinery MVP

**Input**: Design documents from `/specs/001-document-intelligence-refinery-mvp/`
**Prerequisites**: `plan.md` (required), `spec.md` (required), `research.md`, `data-model.md`, `contracts/`

**Tests**: Included because the feature specification defines explicit independent test criteria per user story and measurable success criteria.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and shared scaffolding

- [X] T001 Update backend dependency set in `pyproject.toml`
- [X] T002 Initialize frontend workspace and scripts in `frontend/package.json`
- [X] T003 [P] Add backend environment template in `.env.example`
- [X] T004 [P] Add frontend environment template in `frontend/.env.local.example`
- [X] T005 Create runtime artifact directories and placeholders in `.refinery/.gitkeep`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core architecture required before any user story implementation

**⚠️ CRITICAL**: No user story work starts until this phase is complete

- [X] T006 Implement shared config loading for model/routing policies in `src/utils/rules.py`
- [X] T007 [P] Implement domain-level base schemas and enums in `src/models/common.py`
- [X] T008 [P] Implement document profile and triage-signal schema hardening in `src/models/document_profile.py`
- [X] T009 [P] Implement extraction and provenance schema hardening in `src/models/extracted_document.py`
- [X] T010 Implement extraction ledger model and serialization helpers in `src/models/extraction_ledger.py`
- [X] T011 Implement model selection and job status schemas in `src/models/query.py`
- [X] T012 Implement shared persistence helpers for JSON/JSONL artifacts in `src/utils/ledger.py`
- [X] T013 Implement API bootstrap, dependency wiring, and health route in `src/api/app.py`

**Checkpoint**: Foundation complete and user stories can be implemented independently.

---

## Phase 3: User Story 1 - Classify and Route Documents (Priority: P1) 🎯 MVP

**Goal**: Produce reliable `DocumentProfile` outputs and route extraction strategy by triage signals.

**Independent Test**: Triage correctly classifies representative native, scanned, mixed, and form-fillable documents and persists `.refinery/profiles/{doc_id}.json`.

### Tests for User Story 1

- [X] T014 [P] [US1] Add unit tests for origin/layout classification logic in `tests/unit/test_triage.py`
- [X] T015 [P] [US1] Add integration test for profile artifact persistence in `tests/integration/test_triage_profile_artifact.py`

### Implementation for User Story 1

- [X] T016 [P] [US1] Implement domain classifier strategy extension points in `src/agents/domain_classifier.py`
- [X] T017 [US1] Implement triage signal extraction and profile assembly in `src/agents/triage.py`
- [X] T018 [US1] Implement language detection fallback behavior in `src/utils/language.py`
- [X] T019 [US1] Implement profile write path to `.refinery/profiles/{doc_id}.json` in `src/agents/triage.py`
- [X] T020 [US1] Implement triage CLI execution path in `src/agents/triage.py`
- [X] T021 [US1] Add class-matrix triage validation script for four document classes in `scripts/phase1_triage_matrix.py`

**Checkpoint**: US1 is functional and independently testable.

---

## Phase 4: User Story 2 - Extract Structured Content with Escalation Guard (Priority: P1)

**Goal**: Execute multi-strategy extraction with confidence-gated escalation and auditable ledgering.

**Independent Test**: Low-confidence Strategy A/B runs escalate automatically, and each extraction creates ledger entries with strategy, confidence, cost, and timing.

### Tests for User Story 2

- [X] T022 [P] [US2] Add unit tests for confidence score behavior in `tests/unit/test_confidence_scoring.py`
- [X] T023 [P] [US2] Add contract tests for A->B->C escalation transitions in `tests/contract/test_router_escalation.py`
- [X] T024 [P] [US2] Add integration tests for extraction ledger artifact creation in `tests/integration/test_extraction_ledger_artifact.py`

### Implementation for User Story 2

- [X] T025 [P] [US2] Implement Strategy A extraction fidelity improvements in `src/strategies/fast_text.py`
- [X] T026 [P] [US2] Implement Strategy B Docling adapter normalization in `src/strategies/layout.py`
- [X] T027 [P] [US2] Implement Strategy C vision provider adapter with budget guard in `src/strategies/vision.py`
- [X] T028 [US2] Implement extraction router strategy selection and escalation thresholds in `src/agents/extractor.py`
- [X] T029 [US2] Implement extraction ledger append flow in `src/agents/extractor.py`
- [X] T030 [US2] Implement extraction API endpoint integration in `src/api/app.py`
- [X] T031 [US2] Implement corpus class processing validation for four classes in `scripts/phase2_extraction_matrix.py`
- [X] T032 [US2] Externalize extraction thresholds and escalation policy in `rubric/extraction_rules.yaml`

**Checkpoint**: US2 is functional and independently testable.

---

## Phase 5: User Story 5 - Operate Through a Responsive Full-Stack UI (Priority: P1)

**Goal**: Deliver upload, document list, model controls, and chat/provenance interface in a responsive layout.

**Independent Test**: User uploads a document, monitors processing status, runs chat queries, and sees provenance citations on desktop and mobile viewports.

### Tests for User Story 5

- [X] T033 [P] [US5] Add frontend component tests for left-panel upload/list/config UI in `frontend/tests/components/left-panel.test.tsx`
- [X] T034 [P] [US5] Add frontend integration tests for right-panel chat/provenance rendering in `frontend/tests/integration/chat-provenance.test.tsx`

### Implementation for User Story 5

- [X] T035 [P] [US5] Implement app shell and responsive split layout in `frontend/app/page.tsx`
- [X] T036 [P] [US5] Implement uploader component with progress states in `frontend/components/document-uploader.tsx`
- [X] T037 [P] [US5] Implement stored-documents list and status badges in `frontend/components/document-list.tsx`
- [X] T038 [P] [US5] Implement model configuration panel in `frontend/components/model-config-panel.tsx`
- [X] T039 [P] [US5] Implement chat conversation panel in `frontend/components/chat-panel.tsx`
- [X] T040 [P] [US5] Implement provenance citation card UI in `frontend/components/provenance-card.tsx`
- [X] T041 [US5] Implement frontend API client for upload/status/query/config endpoints in `frontend/lib/api-client.ts`
- [X] T042 [US5] Implement status polling and live updates in `frontend/lib/use-job-status.ts`

**Checkpoint**: US5 is functional and independently testable.

---

## Phase 6: User Story 6 - Dynamic Model Selection and Observability (Priority: P1)

**Goal**: Recommend best model/provider per context, allow override, and trace decision paths.

**Independent Test**: Auto-select and override both work, with model decisions and tool sequence visible in LangSmith-linked response metadata.

### Tests for User Story 6

- [X] T043 [P] [US6] Add unit tests for model recommendation policy scoring in `tests/unit/test_model_selector.py`
- [X] T044 [P] [US6] Add integration tests for override precedence behavior in `tests/integration/test_model_override_flow.py`

### Implementation for User Story 6

- [X] T045 [P] [US6] Implement provider/model abstraction and routing interface in `src/services/model_gateway.py`
- [X] T046 [P] [US6] Implement Ollama provider adapter in `src/services/model_gateway.py`
- [X] T047 [P] [US6] Implement OpenRouter provider adapter in `src/services/model_gateway.py`
- [X] T048 [US6] Implement automatic recommendation policy using document profile and query intent in `src/services/model_gateway.py`
- [X] T049 [US6] Implement model override and fallback behavior in `src/api/app.py`
- [X] T050 [US6] Implement decision persistence to traceable records in `src/utils/ledger.py`
- [X] T051 [US6] Implement LangSmith trace metadata integration in `src/services/tracing.py`

**Checkpoint**: US6 is functional and independently testable.

---

## Phase 7: User Story 3 - Produce RAG-Ready Chunks and PageIndex (Priority: P2)

**Goal**: Create validated LDUs and a navigable PageIndex tree to improve section-first retrieval.

**Independent Test**: Chunk validator enforces all constitution rules and `pageindex_navigate` returns relevant top-3 sections before semantic search.

### Tests for User Story 3

- [X] T052 [P] [US3] Add unit tests for chunk constitution rule enforcement in `tests/unit/test_chunk_validator.py`
- [X] T053 [P] [US3] Add unit tests for PageIndex ranking behavior in `tests/unit/test_pageindex_ranking.py`
- [X] T054 [P] [US3] Add integration test for vector ingestion and section-first retrieval in `tests/integration/test_pageindex_vector_flow.py`

### Implementation for User Story 3

- [X] T055 [P] [US3] Implement LDU generation and chunk typing in `src/agents/chunker.py`
- [X] T056 [US3] Implement chunk validator rules from constitution in `src/agents/chunker.py`
- [X] T057 [US3] Implement content hashing and cross-reference linking in `src/agents/chunker.py`
- [X] T058 [P] [US3] Implement PageIndex schema and traversal helpers in `src/models/pageindex.py`
- [X] T059 [US3] Implement PageIndex builder and hierarchy extraction in `src/agents/indexer.py`
- [X] T060 [US3] Implement section summary and key-entity enrichment in `src/agents/indexer.py`
- [X] T061 [US3] Implement vector store ingestion adapter for LDU payloads in `src/services/vector_store.py`
- [X] T062 [US3] Implement PageIndex artifact persistence to `.refinery/pageindex/{doc_id}.json` in `src/agents/indexer.py`

**Checkpoint**: US3 is functional and independently testable.

---

## Phase 8: User Story 4 - Answer Questions with Full Provenance and Auditability (Priority: P2)

**Goal**: Build query agent tooling with mandatory provenance chain and audit verification mode.

**Independent Test**: Query responses always include provenance citations and audit mode returns `verified` or `unverifiable` with explicit evidence behavior.

### Tests for User Story 4

- [X] T063 [P] [US4] Add contract tests for provenance response schema in `tests/contract/test_query_provenance_contract.py`
- [X] T064 [P] [US4] Add integration tests for LangGraph tool routing sequence in `tests/integration/test_langgraph_query_flow.py`
- [X] T065 [P] [US4] Add integration tests for audit mode verification outcomes in `tests/integration/test_audit_mode.py`

### Implementation for User Story 4

- [X] T066 [P] [US4] Implement query tool contracts in `src/agents/query_tools.py`
- [X] T067 [US4] Implement `pageindex_navigate` tool behavior in `src/agents/query_tools.py`
- [X] T068 [US4] Implement `semantic_search` tool behavior in `src/agents/query_tools.py`
- [X] T069 [US4] Implement `structured_query` over fact table in `src/services/fact_table.py`
- [X] T070 [US4] Implement LangGraph query orchestration in `src/agents/query_agent.py`
- [X] T071 [US4] Enforce provenance chain in synthesized answers in `src/agents/query_agent.py`
- [X] T072 [US4] Implement audit mode claim verification in `src/agents/query_agent.py`
- [X] T073 [US4] Implement query API endpoint and response shaping in `src/api/app.py`

**Checkpoint**: US4 is functional and independently testable.

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Quality hardening and delivery readiness across all stories

- [X] T074 [P] Add end-to-end smoke test for unseen document flow in `tests/integration/test_pipeline_smoke.py`
- [X] T075 [P] Add per-class query demo verification script in `scripts/phase4_query_demo_matrix.py`
- [X] T076 [P] Update full-stack setup and runbook in `README.md`
- [X] T077 [P] Validate quickstart end-to-end and refresh commands in `specs/001-document-intelligence-refinery-mvp/quickstart.md`
- [X] T078 [P] Add interim/final artifact checklist in `docs/DELIVERY_CHECKLIST.md`
- [X] T079 [P] Finalize demo protocol walkthrough in `docs/DEMO_PROTOCOL.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- Setup (Phase 1) has no dependencies.
- Foundational (Phase 2) depends on Setup and blocks all user story phases.
- User stories proceed in priority order after Foundational: US1 -> US2 -> US5 -> US6 -> US3 -> US4.
- Polish (Phase 9) depends on completion of target user stories.

### User Story Dependencies

- US1 depends on Foundational only.
- US2 depends on US1 profile and routing contracts.
- US5 depends on API primitives from US1/US2.
- US6 depends on US5 config controls and API model hooks.
- US3 depends on normalized extraction outputs from US2.
- US4 depends on US3 PageIndex/vector artifacts and US6 model gateway.

### Parallel Opportunities

- In Setup and Foundational, tasks marked `[P]` can execute in parallel.
- In each user story, tests and model/UI component tasks marked `[P]` can execute in parallel.
- US5 UI component tasks can run in parallel after API contract lock.
- US3 chunking and PageIndex schema tasks can run in parallel.

---

## Parallel Example: User Story 1

```bash
T014 tests/unit/test_triage.py
T015 tests/integration/test_triage_profile_artifact.py
T016 src/agents/domain_classifier.py
```

## Parallel Example: User Story 2

```bash
T025 src/strategies/fast_text.py
T026 src/strategies/layout.py
T027 src/strategies/vision.py
```

## Parallel Example: User Story 5

```bash
T036 frontend/components/document-uploader.tsx
T037 frontend/components/document-list.tsx
T038 frontend/components/model-config-panel.tsx
T039 frontend/components/chat-panel.tsx
T040 frontend/components/provenance-card.tsx
```

## Parallel Example: User Story 6

```bash
T045 src/services/model_gateway.py
T046 src/services/model_gateway.py
T047 src/services/model_gateway.py
T051 src/services/tracing.py
```

## Parallel Example: User Story 3

```bash
T052 tests/unit/test_chunk_validator.py
T053 tests/unit/test_pageindex_ranking.py
T055 src/agents/chunker.py
T058 src/models/pageindex.py
```

## Parallel Example: User Story 4

```bash
T063 tests/contract/test_query_provenance_contract.py
T064 tests/integration/test_langgraph_query_flow.py
T065 tests/integration/test_audit_mode.py
T066 src/agents/query_tools.py
```

---

## Implementation Strategy

### MVP First (Recommended)

1. Complete Phase 1 and Phase 2.
2. Complete US1 and validate triage artifacts.
3. Complete US2 and validate escalation + ledger behavior.
4. Complete US5 to deliver usable upload/chat UI.
5. Complete US6 for dynamic model selection and tracing.

### Incremental Delivery

1. Deliver MVP after US1 + US2 + US5.
2. Add US6 for model recommendation/override observability.
3. Add US3 for chunking/PageIndex retrieval quality.
4. Add US4 for full provenance and audit-mode guarantees.
5. Finish with Phase 9 hardening and demo readiness.
