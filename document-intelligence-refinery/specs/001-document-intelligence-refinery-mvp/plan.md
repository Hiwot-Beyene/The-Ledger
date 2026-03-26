# Implementation Plan: Document Intelligence Refinery MVP

**Branch**: `001-document-intelligence-refinery-mvp` | **Date**: 2026-03-06 | **Spec**: `spec.md`
**Input**: Feature specification from `/specs/001-document-intelligence-refinery-mvp/spec.md`

## Summary

Deliver a production-oriented, full-stack Document Intelligence Refinery with five backend pipeline stages and a responsive frontend experience for upload, processing, and provenance-grounded chat. The design uses profile-first routing, confidence-gated escalation (A->B->C), schema-normalized extraction, semantic chunking, PageIndex navigation, LangGraph query orchestration, and dynamic model routing across Ollama/OpenRouter with LangSmith traces.

## Technical Context

**Language/Version**: Python 3.11 backend, TypeScript/Node.js 20 frontend  
**Primary Dependencies**: `pydantic`, `pdfplumber`, `docling`, `pyyaml`, `fasttext-langdetect`, `openai`, `langgraph`, `langchain`, `langsmith`, `chromadb`, `fastapi`, `uvicorn`  
**Frontend Dependencies**: `next`, `react`, `tailwindcss`, `@tanstack/react-query`  
**Storage**: `.refinery/*.json|jsonl`, ChromaDB for vectors, SQLite for structured facts  
**Testing**: `pytest` (unit/contract/integration), frontend integration tests (Playwright or Vitest+RTL)  
**Target Platform**: Linux local deployment  
**Project Type**: Python backend service + Next.js web application  
**Performance Goals**: non-blocking ingestion/extraction jobs, UI progress updates, provenance in every answer  
**Constraints**: cost-capped vision extraction, config-driven routing, auditable provenance, modular/testable stages  
**Scale/Scope**: minimum 12 profiled/extracted docs (3 per target class) with end-to-end query demos

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **I. Classification-First Routing**: PASS. Triage (`DocumentProfile`) remains mandatory before extraction routing.
- **II. Escalation Guard over Silent Failure**: PASS. Router escalates on low confidence and logs strategy sequence.
- **III. Provenance is Non-Negotiable**: PASS. Data model and API contracts require page, bbox, and content hash citations.
- **IV. Structure-Preserving Chunking**: PASS. Chunk validator rules are explicit and enforced before LDU emission.
- **V. Cost-Aware, Config-Driven Engineering**: PASS. Thresholds and budget policy externalized in `rubric/extraction_rules.yaml`.

**Workflow Gate Exception (Documented)**: Constitution references `01_spec.md`/`02_design.md`/`03_tasks.md`, but this repository uses Speckit canonical artifacts `spec.md`/`plan.md`/`tasks.md` for the same feature. No loss of governance semantics; traceability is preserved through canonical files.

## Project Structure

### Documentation (this feature)

```text
specs/001-document-intelligence-refinery-mvp/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
└── tasks.md
```

### Source Code (repository root)

```text
src/
├── agents/
│   ├── triage.py
│   ├── extractor.py
│   ├── chunker.py
│   ├── indexer.py
│   └── query_agent.py
├── strategies/
│   ├── base.py
│   ├── fast_text.py
│   ├── layout.py
│   └── vision.py
├── services/
│   ├── model_gateway.py
│   ├── vector_store.py
│   ├── fact_table.py
│   └── tracing.py
├── api/
│   └── app.py
├── models/
│   ├── common.py
│   ├── document_profile.py
│   ├── extracted_document.py
│   ├── extraction_ledger.py
│   ├── pageindex.py
│   └── query.py
└── utils/
    ├── language.py
    ├── ledger.py
    └── rules.py

frontend/
├── app/
├── components/
├── lib/
└── styles/

tests/
├── contract/
├── integration/
└── unit/
```

**Structure Decision**: Keep single Python project root for pipeline and API, and add `frontend/` Next.js app to support the required split-panel UX and model controls.

## Phase 0 — Research & Clarifications

1. Confirm extraction strategy boundaries and confidence signals from Phase 0 corpus evidence.
2. Finalize dynamic model policy for Ollama/OpenRouter by document class and query intent.
3. Define LangGraph orchestration shape and LangSmith trace payload minimum.
4. Confirm vector and fact-query stack (ChromaDB + SQLite) for local deployment.

Output: `research.md` with all clarification items resolved and no remaining `NEEDS CLARIFICATION` markers.

## Phase 1 — Design & Contracts

1. Finalize data entities and validation contracts in `data-model.md`.
2. Update API and ledger contracts in `contracts/` to match frontend/API flow.
3. Update quickstart to backend + frontend + tracing startup flow.
4. Align artifacts to mandatory demo path: Triage -> Extraction -> PageIndex -> Query with provenance.

Output: `data-model.md`, `contracts/refinery-api.yaml`, `contracts/extraction-ledger.schema.json`, `quickstart.md`.

## Post-Design Constitution Re-check

- **I** PASS: profile-first routing unchanged.
- **II** PASS: escalation guard remains required and testable.
- **III** PASS: provenance chain mandatory in query responses/contracts.
- **IV** PASS: chunk constitution rules captured and enforceable.
- **V** PASS: cost/routing settings remain externalized.

No unapproved constitutional deviations after Phase 1 design.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Constitution naming expects `01_spec.md`/`02_design.md`/`03_tasks.md` | Repository and Speckit command flow standardize on `spec.md`/`plan.md`/`tasks.md` | Duplicating both naming systems introduces drift risk and duplicate source of truth |
