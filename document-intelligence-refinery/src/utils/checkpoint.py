"""Checkpoint save/load for vision extraction resume."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

CHECKPOINT_DIR = Path(".refinery/checkpoints")


def _checkpoint_path(doc_id: str, job_id: str | None = None) -> Path:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"{doc_id}.json"


def save_vision_checkpoint(
    doc_id: str,
    job_id: str | None,
    last_completed_page: int,
    total_cost_usd: float,
    total_prompt_tokens: int,
    total_completion_tokens: int,
    partial_doc: dict[str, Any],
    pdf_path: str | None = None,
) -> None:
    """Persist vision extraction state for resume."""
    path = _checkpoint_path(doc_id, job_id)
    payload = {
        "doc_id": doc_id,
        "job_id": job_id,
        "last_completed_page": last_completed_page,
        "total_cost_usd": total_cost_usd,
        "total_prompt_tokens": total_prompt_tokens,
        "total_completion_tokens": total_completion_tokens,
        "partial_doc": partial_doc,
        "pdf_path": pdf_path,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=0)


def load_vision_checkpoint(
    doc_id: str, job_id: str | None = None, pdf_path: str | None = None
) -> dict[str, Any] | None:
    """Load checkpoint if present and optional pdf_path matches."""
    path = _checkpoint_path(doc_id, None)
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if pdf_path and data.get("pdf_path") and data["pdf_path"] != pdf_path:
            return None
        return data
    except (json.JSONDecodeError, OSError):
        return None


def delete_vision_checkpoint(doc_id: str, job_id: str | None = None) -> bool:
    """Remove checkpoint file. Returns True if deleted."""
    path = _checkpoint_path(doc_id, job_id)
    if path.exists():
        try:
            path.unlink()
            return True
        except OSError:
            pass
    return False


def has_checkpoint(doc_id: str, job_id: str | None = None) -> bool:
    """Return True if a checkpoint exists for this doc."""
    path = _checkpoint_path(doc_id, None)
    return path.exists()
