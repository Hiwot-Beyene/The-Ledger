#!/usr/bin/env -S uv run python
"""Run the excise proclamation query and print the answer. Run from repo root."""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Run from repo root so .refinery/ and env are found
ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

from src.api.app import app

DOC_ID = "616f18e1062c"
QUERY = "by which proclamation number is the excise rates are set?"


def main() -> None:
    client = TestClient(app)
    r = client.post("/query", json={"doc_ids": [DOC_ID], "query": QUERY})
    print(f"Status: {r.status_code}")
    if r.status_code != 200:
        print(r.text)
        sys.exit(1)
    data = r.json()
    answer = data.get("answer", "")
    print(f"Answer:\n{answer}")
    print()
    # Check for proclamation number (expect 1186/2020 for this document)
    import re
    if "1186/2020" in answer:
        print("PASS: Answer contains the correct proclamation number (1186/2020).")
    elif re.search(r"Proclamation\s+No\.?\s*\d{2,5}[/\-]\d{2,4}", answer, re.I):
        print("PASS: Answer contains a proclamation number.")
    else:
        print("FAIL: Answer does not contain a proclamation number (e.g. 1186/2020).")
        sys.exit(1)


if __name__ == "__main__":
    main()
