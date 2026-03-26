# Quickstart: Document Intelligence Refinery MVP (Full-Stack)

## 1) Backend Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .[dev]
```

Set environment variables for model providers and tracing:

```bash
export OLLAMA_BASE_URL=http://localhost:11434
export OPENROUTER_API_KEY=<your_openrouter_key>
export LANGSMITH_API_KEY=<your_langsmith_key>
export LANGSMITH_TRACING=true
```

## 2) Run CLI Triage (Optional Smoke Check)

```bash
python -m src.agents.triage --input data/<document>.pdf --rules rubric/extraction_rules.yaml
```

Expected output:
- Profile JSON in `.refinery/profiles/<doc_id>.json`
- Includes language code/confidence (Amharic-capable detection via FastText)

## 3) Run CLI Extraction Router (Optional Smoke Check)

```bash
python -m src.agents.extractor --input data/<document>.pdf --rules rubric/extraction_rules.yaml
```

Expected output:
- Normalized extraction payload (stdout or configured output file)
- Ledger append in `.refinery/extraction_ledger.jsonl`
- Automatic escalation on low confidence (A→B→C)

## 4) Start Backend API

```bash
uvicorn src.api.app:app --reload --host 0.0.0.0 --port 8000
```

Expected API surface (minimum):
- `POST /documents/upload`
- `GET /documents`
- `POST /documents/{doc_id}/process`
- `GET /documents/{doc_id}/status`
- `GET /documents/{doc_id}/pageindex`
- `POST /query`
- `GET /config/models`
- `POST /config/models`

## 5) Frontend Setup (Next.js)

```bash
cd frontend
npm install
npm run dev
```

Expected UI behavior:
- Left panel: uploader, document list, model configuration
- Right panel: chat + provenance display (page, bbox, content hash)
- Responsive behavior on desktop/mobile

## 6) End-to-End Flow

1. Upload a document from the left panel.
2. Trigger processing and wait for status `ready`.
3. Ask a question in chat.
4. Verify provenance citations in the answer.
5. Toggle model auto-select vs override and rerun query.

## 7) Run tests

```bash
pytest -q
```

```bash
cd frontend
npm test -- --run
```

Expected test scope:
- Triage origin/layout/domain/language behavior
- Confidence scoring and escalation guard behavior
- Chunking, PageIndex, and query provenance contracts
- Model selection and LangGraph tool-routing integration

## 8) Verify challenge artifacts

- `src/models/` contains all required Pydantic schemas including provenance and query models
- `src/agents/` contains `triage.py`, `extractor.py`, `chunker.py`, `indexer.py`, `query_agent.py`
- `src/services/` contains model gateway, vector store, fact table, tracing components
- `src/api/app.py` exposes full backend endpoints
- `frontend/` contains Next.js + Tailwind app
- `rubric/extraction_rules.yaml` includes extraction thresholds and model policy defaults
- `.refinery/profiles/`, `.refinery/extraction_ledger.jsonl`, `.refinery/pageindex/` are populated
- Query demo matrix script emits artifact: `.refinery/phase4_query_demo_matrix.json`
