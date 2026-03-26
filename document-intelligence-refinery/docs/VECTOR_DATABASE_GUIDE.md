# Vector database: ChromaDB – connect and preview

This project uses **ChromaDB** as the vector database, as specified in the documentation. Chunks (LDUs) from processed documents are embedded and stored in ChromaDB for semantic search at query time.

---

## 1. Where ChromaDB is used

- **Storage path:** `.refinery/chroma` (or the path set in `REFINERY_CHROMA_PATH`).
- **Collection name:** `refinery_ldus`.
- **Embeddings:** Local model `all-MiniLM-L6-v2` via `sentence-transformers` (no API key).
- **Code:** `src/services/vector_store.py` defines `ChromaVectorStore`; the API uses it by default in `src/api/app.py`.

---

## 2. Step-by-step: connect and run

### Step 1: Install dependencies

From the project root:

```bash
uv sync
# or: pip install -e ".[dev]"
```

This installs `chromadb` and `sentence-transformers`. The first run will download the embedding model (~90MB).

### Step 2: (Optional) Set the database path

Copy the example env and set the ChromaDB path if you want it somewhere other than `.refinery/chroma`:

```bash
cp .env.example .env
# Edit .env and set, for example:
# REFINERY_CHROMA_PATH=.refinery/chroma
```

If you don’t set it, `.refinery/chroma` is used.

### Step 3: Start the backend

```bash
uv run uvicorn src.api.app:app --reload --host 0.0.0.0 --port 8000
```

The app creates the ChromaDB client and the `refinery_ldus` collection on first use. No separate “start ChromaDB” step is needed; it’s embedded and persists to disk under `REFINERY_CHROMA_PATH`.

### Step 4: Ingest documents so the DB has content

1. Open the frontend (e.g. http://localhost:3000) or use the API.
2. Upload a PDF (e.g. via `POST /documents/upload`).
3. Process it: `POST /documents/{doc_id}/process`.

Processing runs the pipeline and calls `VECTOR_STORE.ingest(doc_id, chunks)`, which writes chunks into ChromaDB. After this, the vector DB is “connected” and filled for that document.

---

## 3. Step-by-step: preview the content of the database

### Option A: API (recommended)

With the backend running (Step 3 above):

**1. Get chunk count (stats)**

```bash
curl -s http://localhost:8000/vector-store/stats
```

Example response:

```json
{"backend": "chroma", "chunk_count": 42}
```

**2. Preview chunks (all or by document)**

Preview up to 50 chunks (default):

```bash
curl -s "http://localhost:8000/vector-store/preview?limit=50"
```

Preview only chunks for a specific document:

```bash
curl -s "http://localhost:8000/vector-store/preview?doc_id=YOUR_DOC_ID&limit=20"
```

Example response:

```json
{
  "backend": "chroma",
  "doc_id_filter": null,
  "limit": 50,
  "chunks": [
    {
      "doc_id": "abc123",
      "chunk_id": "ldu-p1-w0",
      "text": "Revenue increased in Q4..."
    }
  ]
}
```

**3. Use the frontend**

- Upload and process a document.
- Ask a question that triggers semantic search; the query agent uses the same ChromaDB store.

### Option B: Python shell

From the project root:

```bash
uv run python
```

```python
from src.services.vector_store import ChromaVectorStore

store = ChromaVectorStore(persist_dir=".refinery/chroma")

# Chunk count
print(store.count())

# Preview chunks (optional: filter by doc_id)
for c in store.get_all(limit=10):
    print(c["doc_id"], c["chunk_id"], c["text"][:80])
```

### Option C: Inspect the persistence directory

- Path: `.refinery/chroma` (or `REFINERY_CHROMA_PATH`).
- ChromaDB stores SQLite and other files there. Don’t edit them by hand; use the API or `ChromaVectorStore` as above.

---

## 4. Summary

| Goal | Action |
|------|--------|
| **Which vector DB** | ChromaDB (persistent, local). |
| **Connect** | Start the app; ChromaDB uses `REFINERY_CHROMA_PATH` (default `.refinery/chroma`). No separate server. |
| **Add content** | Upload and process documents via the API or frontend. |
| **Preview** | `GET /vector-store/stats` and `GET /vector-store/preview?doc_id=...&limit=...`, or Python with `ChromaVectorStore.get_all()`. |
