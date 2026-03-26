from __future__ import annotations

import json
from pathlib import Path

from src.agents.triage import TriageAgent
from src.utils.rules import DEFAULT_RULES


class _FakePage:
    width = 1000
    height = 1000

    def __init__(self) -> None:
        self.chars = [
            {"x0": 10, "x1": 20, "top": 10, "bottom": 20},
            {"x0": 25, "x1": 35, "top": 10, "bottom": 20},
        ]
        self.images = []
        self.lines = []
        self.rects = []
        self.annots = []

    def extract_words(self) -> list[dict[str, str | float]]:
        return [
            {"text": "This"},
            {"text": "is"},
            {"text": "a"},
            {"text": "test"},
            {"text": "document"},
            {"text": "about"},
            {"text": "tax"},
        ]


class _FakePDF:
    def __init__(self) -> None:
        self.pages = [_FakePage()]

    def __enter__(self) -> "_FakePDF":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_triage_persists_profile_artifact(monkeypatch, tmp_path: Path):
    def _fake_open(_path: str):
        return _FakePDF()

    monkeypatch.setattr("src.agents.triage.pdfplumber.open", _fake_open)
    monkeypatch.chdir(tmp_path)

    (tmp_path / ".refinery" / "profiles").mkdir(parents=True, exist_ok=True)
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%%EOF")

    agent = TriageAgent(DEFAULT_RULES)
    profile = agent.profile_document(pdf_path, persist=True)

    artifact = tmp_path / ".refinery" / "profiles" / f"{profile.doc_id}.json"
    assert artifact.exists()

    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["doc_id"] == profile.doc_id
    assert payload["document_name"] == "sample.pdf"
    assert payload["selected_strategy"] in {"A", "B", "C"}
