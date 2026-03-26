from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.agents.extractor import ExtractionRouter
from src.utils.rules import load_rules


def class_from_filename(name: str) -> str:
    lowered = name.lower()
    for class_name in ("native", "scanned", "mixed", "form"):
        if class_name in lowered:
            return class_name
    return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run extraction matrix for representative class PDFs",
    )
    parser.add_argument("--input-dir", required=True, help="Directory containing class representative PDFs")
    parser.add_argument(
        "--rules",
        default="rubric/extraction_rules.yaml",
        help="Path to extraction rules",
    )
    parser.add_argument(
        "--output",
        default=".refinery/phase2_extraction_matrix.json",
        help="Where to write matrix summary JSON",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {input_dir}")

    router = ExtractionRouter(load_rules(args.rules))
    rows: list[dict[str, str | float | int]] = []

    for pdf_path in sorted(input_dir.glob("*.pdf")):
        extracted, ledger = router.run(pdf_path)
        rows.append(
            {
                "file": pdf_path.name,
                "class": class_from_filename(pdf_path.stem),
                "doc_id": ledger.doc_id,
                "strategy_sequence": "->".join(step.value for step in ledger.strategy_sequence),
                "final_strategy": ledger.final_strategy.value,
                "confidence": ledger.confidence_score,
                "cost_estimate_usd": ledger.cost_estimate_usd,
                "pages_extracted": len(extracted.get("pages", [])),
            }
        )

    if not rows:
        raise SystemExit(f"No PDF files found in {input_dir}")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"Extraction matrix complete for {len(rows)} file(s)")
    print(f"Wrote results to {output}")


if __name__ == "__main__":
    main()
