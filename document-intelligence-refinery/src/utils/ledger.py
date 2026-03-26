from __future__ import annotations

import json
from pathlib import Path


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: str | Path, payload: dict) -> None:
    target = Path(path)
    ensure_parent(target)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def append_jsonl(path: str | Path, payload: dict) -> None:
    target = Path(path)
    ensure_parent(target)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_json(path: str | Path) -> dict:
    target = Path(path)
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_jsonl(path: str | Path) -> list[dict]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict] = []
    with target.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows

def append_model_decision(path: str | Path, payload: dict) -> None:
    append_jsonl(path, payload)
