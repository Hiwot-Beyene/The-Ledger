# Demo Protocol

## Objective

Demonstrate an end-to-end local-first run across triage, extraction, indexing, and query with provenance.

## Steps

1. Start backend API:
   `uvicorn src.api.app:app --reload --host 0.0.0.0 --port 8000`
2. Start frontend:
   `cd frontend && npm run dev`
3. Upload one document from each class (native/scanned/mixed/form-like).
4. Trigger processing and observe status transitions (`uploaded -> ready`).
5. Run at least one query per document and inspect provenance cards.
6. Switch model config from auto-select to explicit override and rerun one query.
7. Run audit mode query and show `verification_status` in response.
8. Verify artifacts in `.refinery/` directories and JSONL ledgers.

## Validation Commands

- `pytest -q`
- `cd frontend && npm test -- --run`
- `python scripts/phase1_triage_matrix.py --input-dir data`
- `python scripts/phase2_extraction_matrix.py --input-dir data`
- `python scripts/phase4_query_demo_matrix.py --doc-id <doc_id> --query "What is revenue?"`
