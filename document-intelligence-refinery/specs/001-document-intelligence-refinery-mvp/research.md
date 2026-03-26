# Research: Document Intelligence Refinery MVP (Phase 0)

## Decision 1: Language detection baseline
- Decision: Use `fasttext-langdetect` (`lid.176`) as the default language detector.
- Rationale: Provides multilingual coverage including Amharic, returns confidence values, and works offline for extraction-time classification.
- Alternatives considered:
  - `lingua-language-detector`: high quality but weaker fit for required language coverage in this corpus.
  - `langdetect`: lightweight, but less stable on OCR-noisy and short text blocks.

## Decision 2: Strategy B extractor
- Decision: Use Docling as the layout-aware extractor in Strategy B.
- Rationale: Existing repo dependencies and prior exploration already include Docling, and normalization into `ExtractedDocument` is straightforward.
- Alternatives considered:
  - MinerU: strong parser quality, but integration complexity is higher for current timeline.

## Decision 3: Strategy C provider pattern
- Decision: Use OpenRouter-compatible multimodal API calls with per-document budget guard.
- Rationale: Enables model flexibility while enforcing spend limits required by constitution and rubric.
- Alternatives considered:
  - Vendor-locked endpoint: rejected for portability and policy transparency reasons.

## Decision 4: Confidence scoring signals
- Decision: Score extraction confidence using char count, char density, image-area ratio, and font metadata presence.
- Rationale: These features are directly measurable with `pdfplumber` and correlate with scanned-vs-digital quality patterns.
- Alternatives considered:
  - Single metric threshold (chars/page): rejected as brittle for table-heavy and mixed-layout files.

## Decision 5: Escalation control
- Decision: Enforce strict `A -> B -> C` escalation on low confidence.
- Rationale: Prevents low-fidelity output from contaminating chunking/index/query stages.
- Alternatives considered:
  - Direct `A -> C` jump: rejected due to unnecessary cost spikes and reduced observability.

## Decision 6: Vector and structured retrieval stack
- Decision: Use ChromaDB for semantic retrieval and SQLite for deterministic structured queries.
- Rationale: Both are local-friendly, reproducible, and map cleanly to the three query tools.
- Alternatives considered:
  - FAISS + ad hoc CSV facts: rejected due to weaker operational ergonomics for local SQL-like analytics.

## Decision 7: Query orchestration framework
- Decision: Use LangGraph for query-agent orchestration with tools `pageindex_navigate`, `semantic_search`, `structured_query`.
- Rationale: Explicit graph edges and tool nodes fit auditable multi-step decisioning and future tool extensibility.
- Alternatives considered:
  - Monolithic single-call agent: rejected because tool decision traceability is weaker.

## Decision 8: Tracing and observability
- Decision: Use LangSmith tracing with required metadata (`query_id`, `doc_id`, `provider`, `model`, `tool_sequence`, `citation_count`).
- Rationale: Needed for debugging model-routing decisions and proving query provenance pipeline behavior.
- Alternatives considered:
  - Local text logs only: rejected for weak graph/tool-level introspection.

## Decision 9: Dynamic model selection policy
- Decision: Implement policy-based recommendation with user override support.
- Rationale: Balances cost, latency, and quality by combining document profile signals with query intent while preserving operator control.
- Alternatives considered:
  - Fixed model per environment: rejected because it cannot adapt to mixed corpus complexity.

## Decision 10: Full-stack interaction model
- Decision: Use FastAPI backend + Next.js/Tailwind frontend with asynchronous job status polling.
- Rationale: Supports responsive upload/process/query workflows and aligns with required left-panel controls and right-panel chat/provenance UI.
- Alternatives considered:
  - CLI-only operation: rejected because it does not satisfy required frontend behavior.

## Clarification Resolution Status

All technical clarifications required for planning are resolved.
No remaining `NEEDS CLARIFICATION` markers.

## Documented References

- Challenge source and rubric requirements: `docs/TRP1 Challenge Week 3_ The Document Intelligence Refinery.md`
- Docling: https://github.com/DS4SD/docling
- MinerU: https://github.com/opendatalab/MinerU
- Marker: https://github.com/VikParuchuri/marker
- PageIndex concept: https://github.com/VectifyAI/PageIndex
- Chunkr: https://github.com/lumina-ai-inc/chunkr
- LangGraph: https://langchain-ai.github.io/langgraph/
- LangSmith: https://docs.smith.langchain.com/
- ChromaDB: https://docs.trychroma.com/
- FAISS: https://faiss.ai/
- pdfplumber: https://github.com/jsvine/pdfplumber
- Tesseract OCR: https://tesseract-ocr.github.io/
- PaddleOCR: https://github.com/PaddlePaddle/PaddleOCR

## Uncertainty and Variation Notes

- Exact confidence thresholds vary by corpus and should be empirically tuned (not universally fixed).
- OCR engine quality differs by language/script and scan quality; adapter-level fallback behavior is implementation-dependent.
- LangGraph usage outside query orchestration is optional; many production systems keep extraction deterministic without agent loops.
- Model routing policy (local Ollama vs OpenRouter) is deployment-specific and should be configurable rather than hard-coded.
