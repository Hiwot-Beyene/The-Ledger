# Document Intelligence Refinery Constitution

## Core Principles

### I. Classification-First Routing (Mandatory)
Every document MUST be triaged into a `DocumentProfile` before extraction begins.
No extractor may run without explicit profile-driven routing inputs (`origin_type`, `layout_complexity`, `estimated_extraction_cost`, `domain_hint`).

### II. Escalation Guard over Silent Failure
Extraction confidence MUST be measured and persisted.
If confidence falls below configured threshold, the system MUST escalate to a higher-fidelity strategy (A→B→C) rather than passing low-quality output downstream.

### III. Provenance is Non-Negotiable
All extracted facts, chunks, and query answers MUST remain source-traceable with page reference and bounding-box coordinates.
Query responses are invalid without a `ProvenanceChain` (`document_name`, `page_number`, `bbox`, `content_hash`).

### IV. Structure-Preserving Chunking
Chunking MUST preserve logical document units and enforce quality rules: table-header association, figure-caption attachment, list integrity, section-parent propagation, and cross-reference links.
Token convenience must never override semantic coherence.

### V. Cost-Aware, Config-Driven Engineering
Vision extraction is budget-constrained and MUST enforce per-document spend caps.
Thresholds, routing rules, and extraction constitution rules MUST be externalized in `rubric/extraction_rules.yaml` to allow onboarding by configuration change, not code rewrite.

## Quality and Compliance Standards

- Pipeline stages MUST expose typed schema boundaries (`DocumentProfile`, `ExtractedDocument`, `LDU`, `PageIndex`, `ProvenanceChain`).
- Every run MUST produce auditable artifacts in `.refinery/` (profiles, extraction ledger, page index outputs).
- The system MUST degrade gracefully (fallback/escalation/partial flags) on uncertain or low-quality inputs.
- Deliverables MUST support the official demo protocol sequence: Triage → Extraction → PageIndex → Query with Provenance.

## Development Workflow and Quality Gates

1. Spec-first workflow is required for all feature work:
	- `01_spec.md` (requirements and acceptance scenarios)
	- `02_design.md` (architecture and contracts)
	- `03_tasks.md` (execution breakdown)
2. Unit tests are required for triage classification and confidence scoring logic.
3. Stage interfaces and artifacts must be independently testable.
4. Any change that weakens provenance, escalation behavior, or chunking rules fails review.

## Governance

This constitution governs all specs and implementation decisions for this repository.
In conflict, this constitution supersedes ad-hoc engineering shortcuts.
Amendments require:
- documented rationale,
- explicit version bump,
- update of affected specs and acceptance criteria.

Compliance checks in review must confirm:
- profile-first routing,
- confidence-gated escalation,
- provenance completeness,
- cost guard enforcement,
- spec-to-task traceability.

**Version**: 1.0.0 | **Ratified**: 2026-03-04 | **Last Amended**: 2026-03-04
