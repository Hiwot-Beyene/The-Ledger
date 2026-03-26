from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.agents.triage import TriageAgent
from src.utils.rules import load_rules

EXPECTED_BY_STEM = {
    "native": "NATIVE_DIGITAL",
    "scanned": "SCANNED_IMAGE",
    "mixed": "MIXED",
    "form": "FORM_FILLABLE",
}


def expected_label_from_name(name: str) -> str | None:
    lowered = name.lower()
    for key, expected in EXPECTED_BY_STEM.items():
        if key in lowered:
            return expected
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run triage classification matrix across representative PDFs",
    )
    parser.add_argument("--input-dir", required=True, help="Directory containing representative PDFs")
    parser.add_argument(
        "--rules",
        default="rubric/extraction_rules.yaml",
        help="Path to extraction rules file",
    )
    parser.add_argument(
        "--output",
        default=".refinery/phase1_triage_matrix.json",
        help="Where to write matrix JSON results",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory does not exist: {input_dir}")

    rules = load_rules(args.rules)
    triage_agent = TriageAgent(rules)

    rows: list[dict[str, str | bool]] = []
    for pdf_path in sorted(input_dir.glob("*.pdf")):
        profile = triage_agent.profile_document(pdf_path, persist=True)
        expected = expected_label_from_name(pdf_path.stem)
        predicted = profile.origin_type.value
        passed = expected == predicted if expected is not None else False

        rows.append(
            {
                "file": pdf_path.name,
                "expected_origin_type": expected or "UNKNOWN",
                "predicted_origin_type": predicted,
                "selected_strategy": profile.selected_strategy.value,
                "pass": passed,
            }
        )

    if not rows:
        raise SystemExit(f"No PDF files found in {input_dir}")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    passed_count = sum(1 for row in rows if row["pass"])
    print(f"Triage matrix complete: {passed_count}/{len(rows)} matched expected origin type")
    print(f"Wrote results to {output_path}")


if __name__ == "__main__":
    main()
