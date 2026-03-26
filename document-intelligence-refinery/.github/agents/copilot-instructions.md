# document-intelligence-refinery Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-03-05

## Active Technologies
- Python 3.11 backend, TypeScript/Node.js 20 frontend + `pydantic`, `pdfplumber`, `docling`, `pyyaml`, `fasttext-langdetect`, `openai`, `langgraph`, `langchain`, `langsmith`, `chromadb`, `fastapi`, `uvicorn` (001-document-intelligence-refinery-mvp)
- `.refinery/*.json|jsonl`, ChromaDB for vectors, SQLite for structured facts (001-document-intelligence-refinery-mvp)

- Python 3.11 + `pydantic>=2`, `pdfplumber`, `docling`, `pyyaml`, `pytest`, `fasttext-langdetect` (Amharic-supported language ID), `openai` (for optional vision strategy via OpenRouter-compatible API) (001-document-intelligence-refinery-mvp)

## Project Structure

```text
src/
tests/
```

## Commands

cd src [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] pytest [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] ruff check .

## Code Style

Python 3.11: Follow standard conventions

## Recent Changes
- 001-document-intelligence-refinery-mvp: Added Python 3.11 backend, TypeScript/Node.js 20 frontend + `pydantic`, `pdfplumber`, `docling`, `pyyaml`, `fasttext-langdetect`, `openai`, `langgraph`, `langchain`, `langsmith`, `chromadb`, `fastapi`, `uvicorn`

- 001-document-intelligence-refinery-mvp: Added Python 3.11 + `pydantic>=2`, `pdfplumber`, `docling`, `pyyaml`, `pytest`, `fasttext-langdetect` (Amharic-supported language ID), `openai` (for optional vision strategy via OpenRouter-compatible API)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
