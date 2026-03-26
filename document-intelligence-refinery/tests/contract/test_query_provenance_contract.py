from fastapi.testclient import TestClient

from src.api.app import app


client = TestClient(app)


def test_query_response_contains_required_provenance_fields(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".refinery").mkdir(parents=True, exist_ok=True)

    response = client.post(
        "/query",
        json={"doc_ids": ["doc-contract"], "query": "What is revenue?"},
    )
    assert response.status_code == 200

    payload = response.json()
    assert "provenance" in payload
    assert payload["provenance"]
    citation = payload["provenance"][0]
    assert {"document_name", "page_number", "bbox", "content_hash"}.issubset(citation)
