from __future__ import annotations

from fastapi.testclient import TestClient

from src.api import app as app_module


client = TestClient(app_module.app)


def test_pipeline_smoke(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".refinery" / "profiles").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".refinery" / "pageindex").mkdir(parents=True, exist_ok=True)

    class _FakeLedger:
        doc_id = "smoke123"

    def _fake_run(_path):
        return {
            "pages": [{"page_number": 1}],
            "ldus": [{"id": "ldu-1", "text": "Revenue up", "content_hash": "hash1"}],
        }, _FakeLedger()

    monkeypatch.setattr(app_module.ExtractionRouter, "run", lambda self, path: _fake_run(path))

    upload = client.post(
        "/documents/upload",
        files={"file": ("smoke.pdf", b"%PDF-1.4\n%%EOF", "application/pdf")},
    )
    assert upload.status_code == 200
    doc_id = upload.json()["doc_id"]

    process = client.post(f"/documents/{doc_id}/process")
    assert process.status_code == 200
    assert process.json()["status"] == "completed"

    query = client.post("/query", json={"doc_ids": [doc_id], "query": "What is revenue?"})
    assert query.status_code == 200
    payload = query.json()
    assert payload["provenance"]
    assert payload["tool_sequence"]
