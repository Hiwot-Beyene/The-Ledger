from __future__ import annotations

import logging
import threading
import uuid
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, File, UploadFile, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import asyncio
import json
from pydantic import BaseModel, Field

from src.agents.extractor import ExtractionRouter
from src.exceptions import BudgetApprovalRequired
from src.utils.checkpoint import has_checkpoint
from src.agents.chunker import merge_ldus_for_ingestion
from src.agents.indexer import build_pageindex, build_pageindex_from_ldus, enrich_pageindex, persist_pageindex, section_texts_from_ldus
from src.agents.query_agent import run_query
from src.models import ModelProvider
from src.services.model_gateway import ModelGateway
from src.services.pricing import get_model_pricing
from src.services.tracing import create_langsmith_trace_id, required_trace_metadata
from src.services.vector_store import get_vector_store
from src.services.fact_table import init_fact_table, delete_facts_by_doc_id
from src.services.fact_extractor import extract_facts_from_chunks
from src.utils.ledger import append_model_decision, read_jsonl, read_json, write_json
from src.utils.rules import deep_merge, load_rules
from src.pipeline import extract_financial_facts


app = FastAPI(
    title="Document Intelligence Refinery API",
    version="0.1.0",
    description="Backend API bootstrap for refinery pipeline and query interfaces",
)
app.add_middleware(
    CORSMiddleware,
    # Default to wildcard so browser clients from any region can call the refinery.
    allow_origins=[os.environ.get("REFINERY_CORS_ORIGINS", "*").strip()],
    allow_credentials=False if os.environ.get("REFINERY_CORS_ORIGINS", "*").strip() == "*" else True,
    allow_methods=["*"],
    allow_headers=["*"],
)

RULES = load_rules("rubric/extraction_rules.yaml")
UPLOAD_DIR = Path("data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

DOCUMENTS: dict[str, dict] = {}
JOBS: dict[str, dict] = {}
DOCUMENTS_PATH = Path(".refinery/documents.json")


def _load_documents() -> None:
    global DOCUMENTS
    if DOCUMENTS_PATH.exists():
        try:
            data = read_json(DOCUMENTS_PATH)
            if isinstance(data, dict):
                DOCUMENTS.update(data)
        except Exception:
            pass
    pageindex_dir = Path(".refinery/pageindex")
    if pageindex_dir.exists():
        for path in pageindex_dir.glob("*.json"):
            doc_id = path.stem
            if doc_id in DOCUMENTS:
                continue
            try:
                data = read_json(path)
                root = data.get("root") or {}
                title = root.get("title") or "unknown.pdf"
                DOCUMENTS[doc_id] = {"doc_id": doc_id, "document_name": title, "status": "ready", "path": ""}
            except Exception:
                continue


def _persist_documents() -> None:
    DOCUMENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    write_json(DOCUMENTS_PATH, DOCUMENTS)


_load_documents()


def _configure_langsmith_tracing() -> None:
    api_key = os.getenv("LANGSMITH_API_KEY", "").strip()
    tracing = os.getenv("LANGSMITH_TRACING", "").strip().lower() in ("true", "1", "yes")
    if api_key and tracing:
        os.environ["LANGCHAIN_TRACING_V2"] = "true"
        os.environ["LANGCHAIN_API_KEY"] = api_key
        project = os.getenv("LANGSMITH_PROJECT", "document-intelligence-refinery")
        os.environ["LANGCHAIN_PROJECT"] = project


_configure_langsmith_tracing()

MODEL_CONFIG: dict = {
    "auto_select": False,
    "override": {"provider": "gemini", "model_name": "gemini-2.5-flash"},
    "vision_override": {"provider": "gemini", "model_name": "gemini-2.5-flash"},
    "summary_override": {"provider": "gemini", "model_name": "gemini-2.5-flash"},
    "vision_source": "gemini",
    "gemini_api_key": "",
    "gemini_base_url": "",
    "openrouter_api_key": "",
    "openrouter_base_url": "",
    "openai_api_key": "",
    "ollama_base_url": "",
    "ollama_api_key": "",
    "max_vision_budget_usd": float(RULES.get("vision", {}).get("max_cost_per_doc_usd", 20.0)),
    "require_approval_over_budget": False,
}
GEMINI_DEFAULT_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"


def _model_runtime_defaults() -> dict:
    return {
        "gemini_base_url": GEMINI_DEFAULT_BASE_URL,
        "gemini_api_key": os.getenv("GEMINI_API_KEY", "").strip(),
    }


VECTOR_STORE = get_vector_store(
    persist_dir=os.getenv("REFINERY_CHROMA_PATH", ".refinery/chroma"),
    use_chroma=True,
)
FACT_DB = Path(".refinery/facts/facts.db")
FACT_DB.parent.mkdir(parents=True, exist_ok=True)
init_fact_table(FACT_DB)


class ExtractRequest(BaseModel):
    document_path: str = Field(min_length=1)


class ExtractResponse(BaseModel):
    extracted: dict
    ledger: dict


class FinancialFactsExtractRequest(BaseModel):
    file_path: str = Field(min_length=1)
    document_type: str = Field(min_length=1, description="income_statement or balance_sheet")


class FinancialFactsExtractResponse(BaseModel):
    facts: dict
    source: str = "docforge.financial_pipeline.v1"


class QueryRequest(BaseModel):
    doc_ids: list[str] = Field(default_factory=list)
    query: str = Field(min_length=1)
    mode: str = "answer"
    model_override: dict | None = None


class ProcessRequest(BaseModel):
    language_hint: str | None = None
    resume: bool = False
    approve: bool = False


class ModelConfigRequest(BaseModel):
    auto_select: bool = False
    override: dict | None = None
    vision_override: dict | None = None
    summary_override: dict | None = None
    vision_source: str | None = None
    gemini_api_key: str | None = None
    gemini_base_url: str | None = None
    openrouter_api_key: str | None = None
    openrouter_base_url: str | None = None
    openai_api_key: str | None = None
    ollama_base_url: str | None = None
    ollama_api_key: str | None = None
    max_vision_budget_usd: float | None = None
    require_approval_over_budget: bool | None = None


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/extract", response_model=ExtractResponse)
def extract_document(payload: ExtractRequest) -> ExtractResponse:
    router = ExtractionRouter(RULES)
    extracted, ledger = router.run(Path(payload.document_path))
    return ExtractResponse(extracted=extracted, ledger=ledger.model_dump())


@app.post("/v1/financial-facts/extract", response_model=FinancialFactsExtractResponse)
def extract_financial_facts_api(payload: FinancialFactsExtractRequest) -> FinancialFactsExtractResponse:
    allowed_types = {"income_statement", "balance_sheet"}
    document_type = payload.document_type.strip().lower()
    if document_type not in allowed_types:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported document_type '{payload.document_type}'. Expected one of: income_statement, balance_sheet",
        )
    try:
        facts = extract_financial_facts(payload.file_path, document_type)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Financial fact extraction failed: {exc}") from exc
    return FinancialFactsExtractResponse(facts=facts)


@app.post("/documents/upload")
def upload_document(file: UploadFile = File(...)) -> dict:
    doc_id = uuid.uuid4().hex[:12]
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = UPLOAD_DIR / f"{doc_id}_{file.filename}"
    content = file.file.read()
    target.write_bytes(content)
    DOCUMENTS[doc_id] = {
        "doc_id": doc_id,
        "document_name": file.filename,
        "status": "uploaded",
        "path": str(target),
    }
    _persist_documents()
    return {
        "doc_id": doc_id,
        "document_name": file.filename,
        "status": "uploaded",
    }


@app.get("/documents")
def list_documents() -> dict:
    out = []
    for item in DOCUMENTS.values():
        doc_id = item["doc_id"]
        status = item["status"]
        job = JOBS.get(doc_id)
        if job and job.get("status") == "running":
            status = "processing"
        out.append({"doc_id": doc_id, "document_name": item["document_name"], "status": status})
    return {"documents": out}


@app.delete("/documents")
def delete_all_documents() -> dict:
    deleted = []
    for doc_id, record in list(DOCUMENTS.items()):
        DOCUMENTS.pop(doc_id, None)
        JOBS.pop(doc_id, None)
        path = record.get("path")
        if path and Path(path).exists():
            try:
                Path(path).unlink()
            except OSError:
                pass
        VECTOR_STORE.delete_by_doc_id(doc_id)
        pageindex_path = Path(f".refinery/pageindex/{doc_id}.json")
        if pageindex_path.exists():
            try:
                pageindex_path.unlink()
            except OSError:
                pass
        delete_facts_by_doc_id(FACT_DB, doc_id)
        deleted.append(doc_id)
    _persist_documents()
    return {"ok": True, "deleted_count": len(deleted), "doc_ids": deleted}


def _run_extraction(doc_id: str, record: dict, job_id: str, resume: bool = False, approve: bool = False) -> None:
    try:
        JOBS[doc_id] = {
            "job_id": job_id,
            "doc_id": doc_id,
            "stage": "extracting",
            "status": "running",
            "progress_percent": 10,
            "budget_status": "under_cap",
            "approval_required": False,
            "can_resume": False,
            "estimated_cost_usd": None,
            "budget_cap_usd": None,
        }
        runtime_model_base = {
            "auto_select": MODEL_CONFIG.get("auto_select", False),
            "override": MODEL_CONFIG.get("override"),
            "gemini_api_key": MODEL_CONFIG.get("gemini_api_key"),
            "gemini_base_url": MODEL_CONFIG.get("gemini_base_url") or GEMINI_DEFAULT_BASE_URL,
            "vision_override": MODEL_CONFIG.get("vision_override"),
            "openrouter_api_key": MODEL_CONFIG.get("openrouter_api_key"),
            "openai_api_key": MODEL_CONFIG.get("openai_api_key"),
            "max_vision_budget_usd": MODEL_CONFIG.get("max_vision_budget_usd"),
            "job_id": job_id,
            "doc_id": doc_id,
            "resume_from_checkpoint": resume,
            "require_approval_over_budget": MODEL_CONFIG.get("require_approval_over_budget", False)
            and not approve,
            "approve_run": approve,
        }
        language_hint = (record.get("language_hint") or "").strip().lower()
        runtime_rules = deep_merge(RULES, {"runtime_model": runtime_model_base})
        router = ExtractionRouter(runtime_rules)
        JOBS[doc_id]["progress_percent"] = 30
        extracted, entry = router.run(record["path"], language_hint=record.get("language_hint"))
        JOBS[doc_id]["budget_status"] = getattr(entry, "budget_status", "under_cap") or "under_cap"
        if JOBS[doc_id]["budget_status"] == "cap_reached":
            JOBS[doc_id]["can_resume"] = has_checkpoint(doc_id)

        raw_ldus = extracted.get("ldus", [])
        chunks = merge_ldus_for_ingestion(raw_ldus)
        VECTOR_STORE.ingest(doc_id=doc_id, chunks=chunks, document_title=record.get("document_name") or "")
        extract_facts_from_chunks(FACT_DB, doc_id, chunks)

        JOBS[doc_id]["progress_percent"] = 70
        JOBS[doc_id]["stage"] = "indexing"
        pages = extracted.get("pages") or []
        total_pages = len(pages) if pages else None
        pageindex = build_pageindex_from_ldus(
            doc_id=doc_id,
            document_name=record["document_name"],
            chunks=chunks,
            total_pages=total_pages,
        )
        use_llm_summary = RULES.get("pageindex", {}).get("use_llm_summary", False)
        if use_llm_summary:
            section_texts = section_texts_from_ldus(pageindex, chunks)
            try:
                summary_runtime = {**_model_runtime_defaults(), **MODEL_CONFIG, "live_model_calls": True}
                gateway = ModelGateway(RULES, runtime_config=summary_runtime)
                pageindex_cfg = RULES.get("pageindex", {})
                max_enrich = pageindex_cfg.get("max_sections_to_enrich")
                max_workers = pageindex_cfg.get("enrich_max_workers")
                pageindex = enrich_pageindex(
                    pageindex,
                    section_texts,
                    gateway,
                    doc_id,
                    override=MODEL_CONFIG.get("summary_override") or MODEL_CONFIG.get("override"),
                    max_sections_to_enrich=max_enrich,
                    max_workers=max_workers,
                )
            except Exception as e:
                logging.warning("PageIndex enrichment failed (summary/entities may be placeholders): %s", e)
        persist_pageindex(pageindex, Path(f".refinery/pageindex/{doc_id}.json"))

        record["status"] = "ready"
        cost_usd = getattr(entry, "cost_estimate_usd", None)
        processing_ms = getattr(entry, "processing_time_ms", None)
        prompt_tokens = getattr(entry, "prompt_tokens", None)
        completion_tokens = getattr(entry, "completion_tokens", None)
        JOBS[doc_id] = {
            "job_id": job_id,
            "doc_id": doc_id,
            "stage": "completed",
            "status": "completed",
            "progress_percent": 100,
            "budget_status": JOBS.get(doc_id, {}).get("budget_status", "under_cap"),
            "approval_required": False,
            "can_resume": False,
            "cost_estimate_usd": round(cost_usd, 6) if cost_usd is not None else None,
            "processing_time_ms": processing_ms,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
        }
        _persist_documents()
    except BudgetApprovalRequired as e:
        JOBS[doc_id] = {
            "job_id": job_id,
            "doc_id": doc_id,
            "stage": "approval_required",
            "status": "approval_required",
            "progress_percent": 0,
            "budget_status": "approval_required",
            "approval_required": True,
            "estimated_cost_usd": round(e.estimated_cost_usd, 6),
            "budget_cap_usd": round(e.budget_cap_usd, 6),
            "page_count": e.page_count,
            "can_resume": False,
            "error": str(e),
        }
        _persist_documents()
    except Exception as e:
        record["status"] = "error"
        JOBS[doc_id] = {
            "job_id": job_id,
            "doc_id": doc_id,
            "stage": "failed",
            "status": "failed",
            "progress_percent": 0,
            "error": str(e),
            "budget_status": JOBS.get(doc_id, {}).get("budget_status"),
            "approval_required": False,
            "can_resume": has_checkpoint(doc_id),
        }
        _persist_documents()


@app.post("/documents/{doc_id}/process")
def process_document(doc_id: str, body: ProcessRequest | None = Body(default=None)) -> dict:
    if doc_id not in DOCUMENTS:
        raise HTTPException(status_code=404, detail="Document not found")
    record = DOCUMENTS[doc_id]
    body = body or ProcessRequest()
    if body.language_hint:
        record["language_hint"] = body.language_hint.strip().lower()
    job_id = f"job-{uuid.uuid4().hex[:10]}"
    JOBS[doc_id] = {
        "job_id": job_id,
        "doc_id": doc_id,
        "stage": "queued",
        "status": "running",
        "progress_percent": 0,
        "budget_status": "under_cap",
        "approval_required": False,
        "can_resume": False,
    }
    thread = threading.Thread(
        target=_run_extraction,
        args=(doc_id, record, job_id),
        kwargs={"resume": body.resume, "approve": body.approve},
        daemon=True,
    )
    thread.start()
    return {
        "job_id": job_id,
        "doc_id": doc_id,
        "stage": "queued",
        "status": "running",
        "progress_percent": 0,
        "budget_status": "under_cap",
        "approval_required": False,
        "can_resume": False,
    }


def _vision_pricing_for_status() -> tuple[float, float, str]:
    """Return (input_per_1m_usd, output_per_1m_usd, model_name) for the configured vision model."""
    cfg = RULES.get("model_selection", {})
    provider = ModelProvider(str(cfg.get("vision_provider", "gemini")))
    model_name = str(cfg.get("vision_model", "gemini-2.5-flash"))
    override = MODEL_CONFIG.get("vision_override")
    if override:
        provider = ModelProvider(str(override.get("provider", provider.value)))
        model_name = str(override.get("model_name", model_name))
    runtime = {
        "gemini_base_url": MODEL_CONFIG.get("gemini_base_url") or GEMINI_DEFAULT_BASE_URL,
        "gemini_api_key": MODEL_CONFIG.get("gemini_api_key") or os.getenv("GEMINI_API_KEY", "").strip(),
    }
    in_p, out_p = get_model_pricing(provider, model_name, RULES, runtime)
    return in_p, out_p, model_name


@app.get("/documents/{doc_id}/status")
def document_status(doc_id: str) -> dict:
    default = {
        "job_id": "pending",
        "doc_id": doc_id,
        "stage": "queued",
        "status": "queued",
        "progress_percent": 0,
        "budget_status": "under_cap",
        "approval_required": False,
        "can_resume": False,
        "estimated_cost_usd": None,
        "budget_cap_usd": None,
        "cost_estimate_usd": None,
        "processing_time_ms": None,
        "prompt_tokens": None,
        "completion_tokens": None,
        "vision_input_per_1m_usd": None,
        "vision_output_per_1m_usd": None,
        "vision_model_name": None,
    }
    out = dict(JOBS.get(doc_id, default))
    if out.get("status") == "completed":
        try:
            in_p, out_p, model_name = _vision_pricing_for_status()
            out["vision_input_per_1m_usd"] = round(in_p, 4)
            out["vision_output_per_1m_usd"] = round(out_p, 4)
            out["vision_model_name"] = model_name
        except Exception:
            pass
    return out


@app.get("/documents/{doc_id}/events")
async def document_events(doc_id: str):
    """SSE stream: emits current job status every 2s until job completes or 5 min timeout."""

    async def stream():
        for _ in range(150):
            status = JOBS.get(
                doc_id,
                {
                    "job_id": "pending",
                    "doc_id": doc_id,
                    "stage": "queued",
                    "status": "queued",
                    "progress_percent": 0,
                    "budget_status": "under_cap",
                    "approval_required": False,
                    "can_resume": False,
                },
            )
            yield f"data: {json.dumps(status)}\n\n"
            if status.get("status") in ("completed", "failed", "approval_required"):
                break
            await asyncio.sleep(2)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.delete("/documents/{doc_id}")
def delete_document(doc_id: str) -> dict:
    if doc_id not in DOCUMENTS:
        raise HTTPException(status_code=404, detail="Document not found")
    record = DOCUMENTS.pop(doc_id)
    JOBS.pop(doc_id, None)
    path = record.get("path")
    if path and Path(path).exists():
        try:
            Path(path).unlink()
        except OSError:
            pass
    VECTOR_STORE.delete_by_doc_id(doc_id)
    pageindex_path = Path(f".refinery/pageindex/{doc_id}.json")
    if pageindex_path.exists():
        try:
            pageindex_path.unlink()
        except OSError:
            pass
    delete_facts_by_doc_id(FACT_DB, doc_id)
    _persist_documents()
    return {"ok": True, "doc_id": doc_id}


@app.get("/documents/{doc_id}/pageindex")
def get_pageindex(doc_id: str) -> dict:
    from src.utils.ledger import read_json

    path = Path(f".refinery/pageindex/{doc_id}.json")
    if not path.exists():
        return {"doc_id": doc_id, "root": {"section_id": "missing", "title": "missing", "page_start": 1, "page_end": 1, "child_sections": []}}
    return read_json(path)


@app.get("/ledger/{doc_id}")
def get_doc_ledger(doc_id: str) -> dict:
    entries = read_jsonl(Path(".refinery/extraction_ledger.jsonl"))
    return {"entries": [row for row in entries if row.get("doc_id") == doc_id]}


@app.get("/vector-store/stats")
def vector_store_stats() -> dict:
    """Return count of chunks in the vector database (ChromaDB)."""
    backend = "chroma" if type(VECTOR_STORE).__name__ == "ChromaVectorStore" else "memory"
    return {"backend": backend, "chunk_count": VECTOR_STORE.count()}


@app.get("/vector-store/preview")
def vector_store_preview(doc_id: str | None = None, limit: int = 50) -> dict:
    """Preview chunks in the vector database. Optional doc_id filter."""
    chunks = VECTOR_STORE.get_all(doc_id=doc_id, limit=limit)
    backend = "chroma" if type(VECTOR_STORE).__name__ == "ChromaVectorStore" else "memory"
    return {"backend": backend, "doc_id_filter": doc_id, "limit": limit, "chunks": chunks}


def _resolve_catalog_defaults(providers: list[dict], cfg: dict | None = None) -> dict:
    cfg = cfg or RULES.get("model_selection", {})

    def _first(provider_key: str, prefer_vision: bool = False) -> str:
        entry = next((p for p in providers if p.get("provider") == provider_key), None)
        models = (entry or {}).get("models") or []
        if not models:
            return str(
                cfg.get(
                    "vision_model" if prefer_vision else "default_model",
                    "gemini-2.5-flash",
                )
            )
        if prefer_vision:
            vision_like = [m for m in models if "gemini" in m.lower() or "vision" in m.lower()]
            return vision_like[0] if vision_like else models[0]
        configured = str(cfg.get("default_model", "gemini-2.5-flash"))
        if configured in models:
            return configured
        return models[0]

    qp = cfg.get("default_provider", "gemini")
    vp = cfg.get("vision_provider", "gemini")
    return {
        "provider": qp,
        "model": _first(qp, False),
        "vision_provider": vp,
        "vision_model": _first(vp, True),
    }


@app.get("/config/models")
def get_model_config() -> dict:
    cfg = RULES.get("model_selection", {})
    local_runtime = {**MODEL_CONFIG, **_model_runtime_defaults()}
    gateway = ModelGateway(RULES, runtime_config=local_runtime)
    providers, discovery_errors = gateway.discover_catalog()
    defaults = _resolve_catalog_defaults(providers, cfg)

    return {
        "providers": providers,
        "discovery_errors": discovery_errors,
        "default_policy": "auto",
        "defaults": defaults,
        "active": {
            "auto_select": MODEL_CONFIG.get("auto_select", False),
            "override": MODEL_CONFIG.get("override"),
            "vision_override": MODEL_CONFIG.get("vision_override"),
            "summary_override": MODEL_CONFIG.get("summary_override"),
            "vision_source": MODEL_CONFIG.get("vision_source", "local"),
            "has_gemini_api_key": bool(MODEL_CONFIG.get("gemini_api_key") or os.getenv("GEMINI_API_KEY", "").strip()),
            "gemini_base_url": MODEL_CONFIG.get("gemini_base_url") or GEMINI_DEFAULT_BASE_URL,
            "has_openrouter_api_key": bool(MODEL_CONFIG.get("openrouter_api_key")),
            "openrouter_base_url": MODEL_CONFIG.get("openrouter_base_url") or None,
            "has_openai_api_key": bool(MODEL_CONFIG.get("openai_api_key")),
            "has_ollama_api_key": bool(os.getenv("OLLAMA_API_KEY", "").strip()),
            "max_vision_budget_usd": MODEL_CONFIG.get("max_vision_budget_usd"),
            "require_approval_over_budget": MODEL_CONFIG.get("require_approval_over_budget", False),
        },
    }


@app.post("/config/models")
def set_model_config(payload: ModelConfigRequest) -> dict:
    MODEL_CONFIG["auto_select"] = payload.auto_select
    MODEL_CONFIG["override"] = payload.override
    MODEL_CONFIG["vision_override"] = payload.vision_override
    if payload.summary_override is not None:
        MODEL_CONFIG["summary_override"] = payload.summary_override
    if payload.vision_source is not None:
        MODEL_CONFIG["vision_source"] = payload.vision_source.strip().lower() or "gemini"
    if payload.gemini_api_key is not None:
        MODEL_CONFIG["gemini_api_key"] = payload.gemini_api_key.strip()
    if payload.gemini_base_url is not None:
        MODEL_CONFIG["gemini_base_url"] = payload.gemini_base_url.strip() or GEMINI_DEFAULT_BASE_URL
    if payload.openrouter_api_key is not None:
        MODEL_CONFIG["openrouter_api_key"] = payload.openrouter_api_key.strip()
    if payload.openrouter_base_url is not None:
        MODEL_CONFIG["openrouter_base_url"] = payload.openrouter_base_url.strip() or ""
    if payload.openai_api_key is not None:
        MODEL_CONFIG["openai_api_key"] = payload.openai_api_key.strip()
    if payload.ollama_base_url is not None:
        MODEL_CONFIG["ollama_base_url"] = payload.ollama_base_url.strip() or ""
    if payload.max_vision_budget_usd is not None:
        MODEL_CONFIG["max_vision_budget_usd"] = payload.max_vision_budget_usd
    if payload.require_approval_over_budget is not None:
        MODEL_CONFIG["require_approval_over_budget"] = payload.require_approval_over_budget
    masked = {
        "auto_select": MODEL_CONFIG.get("auto_select"),
        "override": MODEL_CONFIG.get("override"),
        "vision_override": MODEL_CONFIG.get("vision_override"),
        "summary_override": MODEL_CONFIG.get("summary_override"),
        "vision_source": MODEL_CONFIG.get("vision_source", "gemini"),
        "has_gemini_api_key": bool(MODEL_CONFIG.get("gemini_api_key") or os.getenv("GEMINI_API_KEY", "").strip()),
        "gemini_base_url": MODEL_CONFIG.get("gemini_base_url") or GEMINI_DEFAULT_BASE_URL,
        "has_openrouter_api_key": bool(MODEL_CONFIG.get("openrouter_api_key")),
        "openrouter_base_url": MODEL_CONFIG.get("openrouter_base_url") or None,
        "has_openai_api_key": bool(MODEL_CONFIG.get("openai_api_key")),
        "has_ollama_api_key": bool(os.getenv("OLLAMA_API_KEY", "").strip()),
        "max_vision_budget_usd": MODEL_CONFIG.get("max_vision_budget_usd"),
        "require_approval_over_budget": MODEL_CONFIG.get("require_approval_over_budget", False),
    }
    return {"ok": True, "config": masked}


@app.post("/query")
def query(payload: QueryRequest) -> dict:
    query_id = f"q-{uuid.uuid4().hex[:10]}"
    doc_id = payload.doc_ids[0] if payload.doc_ids else "unknown"

    override = None
    if payload.model_override:
        override = payload.model_override
    elif not MODEL_CONFIG.get("auto_select"):
        override = MODEL_CONFIG.get("override")
    else:
        gateway_temp = ModelGateway(RULES, runtime_config={**_model_runtime_defaults(), **MODEL_CONFIG})
        providers, _ = gateway_temp.discover_catalog()
        defaults = _resolve_catalog_defaults(providers)
        override = {"provider": defaults["provider"], "model_name": defaults["model"]}

    gateway = ModelGateway(RULES, runtime_config={**_model_runtime_defaults(), **MODEL_CONFIG})
    pageindex_path = Path(f".refinery/pageindex/{doc_id}.json")
    document_name = DOCUMENTS.get(doc_id, {}).get("document_name") or "unknown.pdf"
    if pageindex_path.exists():
        pageindex_payload = read_json(pageindex_path)
        from src.models.pageindex import PageIndex

        pageindex = PageIndex.model_validate(pageindex_payload)
        if document_name != "unknown.pdf":
            pageindex.root.title = document_name
    else:
        pageindex = build_pageindex(
            doc_id=doc_id,
            document_name=document_name,
            pages=[1],
        )

    result = run_query(
        query=payload.query,
        doc_ids=payload.doc_ids,
        pageindex=pageindex,
        vector_store=VECTOR_STORE,
        model_gateway=gateway,
        db_path=str(FACT_DB),
        mode=payload.mode,
        override=override,
    )

    decision = result["model_decision"]
    tool_sequence = result["tool_sequence"]
    trace_id = result["langsmith_trace_id"]
    citations = result["provenance"]

    append_model_decision(
        Path(".refinery/model_decisions.jsonl"),
        {
            "query_id": query_id,
            "doc_id": doc_id,
            "model_decision": decision,
            "tool_sequence": tool_sequence,
            "trace_metadata": required_trace_metadata(
                query_id=query_id,
                doc_id=doc_id,
                provider=decision["provider"],
                model=decision["model_name"],
                tool_sequence=tool_sequence,
                citation_count=len(citations),
            ),
            "langsmith_trace_id": trace_id,
        },
    )

    return result
