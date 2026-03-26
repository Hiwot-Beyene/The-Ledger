import json
from pathlib import Path

from src.utils.ledger import append_jsonl


def test_append_jsonl_writes_single_record(tmp_path):
    out = tmp_path / "ledger.jsonl"
    payload = {"doc_id": "abc", "confidence_score": 0.7}
    append_jsonl(out, payload)

    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["doc_id"] == "abc"
