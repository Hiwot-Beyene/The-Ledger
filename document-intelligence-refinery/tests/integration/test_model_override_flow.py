from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from src.api.app import MODEL_CONFIG, app


client = TestClient(app)


def test_query_uses_override_from_request(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".refinery").mkdir(parents=True, exist_ok=True)

    MODEL_CONFIG["auto_select"] = True
    MODEL_CONFIG["override"] = None

    response = client.post(
        "/query",
        json={
            "doc_ids": ["doc-1"],
            "query": "Summarize this.",
            "model_override": {"provider": "openrouter", "model_name": "openai/gpt-4o-mini"},
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["model_decision"]["provider"] == "openrouter"
    assert payload["model_decision"]["mode"] == "user_override"


def test_query_uses_saved_override_when_auto_disabled(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".refinery").mkdir(parents=True, exist_ok=True)

    config_response = client.post(
        "/config/models",
        json={
            "auto_select": False,
            "override": {"provider": "ollama", "model_name": "llama3.1:8b"},
        },
    )
    assert config_response.status_code == 200

    response = client.post(
        "/query",
        json={
            "doc_ids": ["doc-2"],
            "query": "List key points.",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["model_decision"]["provider"] == "ollama"
    assert payload["model_decision"]["mode"] == "user_override"

    decision_log = tmp_path / ".refinery" / "model_decisions.jsonl"
    assert decision_log.exists()
    rows = [json.loads(line) for line in decision_log.read_text(encoding="utf-8").splitlines() if line]
    assert rows
