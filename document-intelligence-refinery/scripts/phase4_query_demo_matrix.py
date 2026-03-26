from __future__ import annotations

import argparse
import json
from pathlib import Path

from fastapi.testclient import TestClient

from src.api.app import app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run query demo matrix for a document")
    parser.add_argument("--doc-id", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--output", default=".refinery/phase4_query_demo_matrix.json")
    args = parser.parse_args()

    client = TestClient(app)
    answer = client.post("/query", json={"doc_ids": [args.doc_id], "query": args.query})
    audit = client.post("/query", json={"doc_ids": [args.doc_id], "query": args.query, "mode": "audit"})

    payload = {
        "doc_id": args.doc_id,
        "query": args.query,
        "answer_mode": answer.json(),
        "audit_mode": audit.json(),
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Query demo matrix written to {output}")


if __name__ == "__main__":
    main()
