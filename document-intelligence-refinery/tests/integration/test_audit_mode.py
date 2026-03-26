from fastapi.testclient import TestClient

from src.api.app import app


client = TestClient(app)


def test_audit_mode_returns_verification_status(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".refinery").mkdir(parents=True, exist_ok=True)

    response = client.post(
        "/query",
        json={"doc_ids": ["doc-audit"], "query": "Verify revenue claim", "mode": "audit"},
    )
    assert response.status_code == 200

    payload = response.json()
    assert payload["verification_status"] in {"verified", "partial", "unverifiable"}
