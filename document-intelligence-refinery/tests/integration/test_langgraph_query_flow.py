from fastapi.testclient import TestClient

from src.api.app import app


client = TestClient(app)


def test_query_tool_sequence_is_recorded(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".refinery").mkdir(parents=True, exist_ok=True)

    response = client.post(
        "/query",
        json={"doc_ids": ["doc-graph"], "query": "Find revenue highlights"},
    )
    assert response.status_code == 200

    payload = response.json()
    assert "pageindex_navigate" in payload["tool_sequence"]
    assert "semantic_search" in payload["tool_sequence"]
    trace_id = payload["langsmith_trace_id"]
    assert trace_id and (trace_id.startswith("ls-") or len(trace_id) == 36)
