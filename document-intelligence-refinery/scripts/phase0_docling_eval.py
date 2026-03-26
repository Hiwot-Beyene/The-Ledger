import argparse
import json
import time
import gc
from pathlib import Path

from docling.document_converter import DocumentConverter
from tqdm import tqdm


def load_processed_documents(metrics_path: Path) -> set[str]:
    processed = set()
    if not metrics_path.exists():
        return processed

    with metrics_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            doc_name = row.get("document")
            if doc_name:
                processed.add(doc_name)

    return processed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=str, default="data")
    parser.add_argument("--out-dir", type=str,
                        default=".refinery/phase0/docling")
    parser.add_argument("--flat-only", action="store_true",
                        help="Only read PDFs directly under data-dir (no recursive scan).")
    parser.add_argument("--batch-size", type=int, default=10,
                        help="Number of PDFs to process in this run.")
    parser.add_argument("--batch-index", type=int, default=0,
                        help="Zero-based batch index. start=batch-index*batch-size.")
    parser.add_argument("--resume", action="store_true",
                        help="Skip documents already present in docling_metrics.jsonl.")
    parser.add_argument("--restart-every", type=int, default=5,
                        help="Recreate DocumentConverter every N processed files to reduce memory growth.")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics_path = out_dir / "docling_metrics.jsonl"
    processed_docs = load_processed_documents(
        metrics_path) if args.resume else set()

    if args.flat_only:
        pdfs = sorted(data_dir.glob("*.pdf"))
    else:
        pdfs = sorted(data_dir.rglob("*.pdf"))

    start = args.batch_index * args.batch_size
    end = start + args.batch_size
    batch_pdfs = pdfs[start:end]

    if not batch_pdfs:
        print(
            f"No PDFs found for batch-index={args.batch_index}, batch-size={args.batch_size}")
        return

    converter = DocumentConverter()
    converted_in_run = 0

    with metrics_path.open("a", encoding="utf-8") as f:
        for pdf in tqdm(batch_pdfs, desc=f"Docling batch {args.batch_index}"):
            if args.resume and pdf.name in processed_docs:
                continue

            md_path = out_dir / f"{pdf.stem}.md"

            if args.resume and md_path.exists():
                row = {
                    "document": pdf.name,
                    "status": "skipped_existing_markdown",
                    "error": None,
                    "seconds": 0.0,
                    "markdown_path": str(md_path),
                }
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                f.flush()
                continue

            if args.restart_every > 0 and converted_in_run > 0 and converted_in_run % args.restart_every == 0:
                del converter
                gc.collect()
                converter = DocumentConverter()

            t0 = time.time()
            row = {"document": pdf.name, "status": "ok", "error": None}
            try:
                result = converter.convert(str(pdf))
                md = result.document.export_to_markdown()
                elapsed = time.time() - t0

                md_path = out_dir / f"{pdf.stem}.md"
                md_path.write_text(md, encoding="utf-8")

                table_lines = sum(1 for line in md.splitlines()
                                  if line.strip().startswith("|"))
                heading_lines = sum(1 for line in md.splitlines()
                                    if line.strip().startswith("#"))

                row.update(
                    {
                        "seconds": elapsed,
                        "markdown_chars": len(md),
                        "table_line_count": table_lines,
                        "heading_line_count": heading_lines,
                        "markdown_path": str(md_path),
                    }
                )
            except Exception as e:
                row.update({"status": "error", "error": str(
                    e), "seconds": time.time() - t0})

            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            f.flush()
            converted_in_run += 1

    print(f"Saved/updated: {metrics_path}")
    print(f"Saved markdown files in: {out_dir}")


if __name__ == "__main__":
    main()
