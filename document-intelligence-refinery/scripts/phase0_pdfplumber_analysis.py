import argparse
import json
from pathlib import Path

import pandas as pd
import pdfplumber
from tqdm import tqdm


def safe_image_area(img):
    x0 = img.get("x0", 0) or 0
    x1 = img.get("x1", 0) or 0
    top = img.get("top", 0) or 0
    bottom = img.get("bottom", 0) or 0
    w = max(0.0, x1 - x0)
    h = max(0.0, bottom - top)
    if w > 0 and h > 0:
        return w * h
    # fallback
    return float((img.get("width", 0) or 0) * (img.get("height", 0) or 0))


def analyze_pdf(pdf_path: Path):
    page_rows = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            width = float(page.width or 0)
            height = float(page.height or 0)
            page_area = max(width * height, 1.0)

            chars = page.chars or []
            images = page.images or []
            lines = page.lines or []
            rects = page.rects or []

            char_count = len(chars)
            char_density = char_count / page_area

            char_bbox_area = 0.0
            for c in chars:
                x0, x1 = c.get("x0", 0), c.get("x1", 0)
                top, bottom = c.get("top", 0), c.get("bottom", 0)
                char_bbox_area += max(0.0, (x1 - x0)) * \
                    max(0.0, (bottom - top))

            text_coverage = min(char_bbox_area / page_area, 1.0)
            whitespace_ratio = 1.0 - text_coverage

            image_area = sum(safe_image_area(img) for img in images)
            image_area_ratio = min(image_area / page_area, 1.0)

            has_font_meta = int(any(c.get("fontname") for c in chars))

            # simple scanned indicator
            scanned_likely = int(char_count < 30 and image_area_ratio > 0.5)

            page_rows.append(
                {
                    "document": pdf_path.name,
                    "page_number": i,
                    "page_width": width,
                    "page_height": height,
                    "page_area": page_area,
                    "char_count": char_count,
                    "char_density": char_density,
                    "text_coverage": text_coverage,
                    "whitespace_ratio": whitespace_ratio,
                    "image_area_ratio": image_area_ratio,
                    "line_count": len(lines),
                    "rect_count": len(rects),
                    "has_font_meta": has_font_meta,
                    "scanned_likely": scanned_likely,
                }
            )
    return page_rows


def classify_origin(doc_df: pd.DataFrame):
    avg_chars = doc_df["char_count"].mean()
    avg_img_ratio = doc_df["image_area_ratio"].mean()
    scanned_pages_ratio = doc_df["scanned_likely"].mean()

    dominant_threshold = 0.85

    if scanned_pages_ratio >= dominant_threshold:
        return "scanned_image"
    if scanned_pages_ratio > (1.0 - dominant_threshold):
        return "mixed"
    if avg_chars > 100 and avg_img_ratio < 0.5:
        return "native_digital"
    return "mixed"


def classify_layout(doc_df: pd.DataFrame):
    avg_lines_rects = (doc_df["line_count"] + doc_df["rect_count"]).mean()
    avg_whitespace = doc_df["whitespace_ratio"].mean()

    if avg_lines_rects > 120:
        return "table_heavy"
    if avg_whitespace > 0.97:
        return "figure_heavy"
    # placeholder heuristic for Phase 0
    return "mixed"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=str, default="data")
    parser.add_argument("--out-dir", type=str,
                        default=".refinery/phase0/pdfplumber")
    parser.add_argument("--flat-only", action="store_true",
                        help="Only read PDFs directly under data-dir (no recursive scan).")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.flat_only:
        pdfs = sorted(data_dir.glob("*.pdf"))
    else:
        pdfs = sorted(data_dir.rglob("*.pdf"))

    all_rows = []

    for pdf_path in tqdm(pdfs, desc="Analyzing PDFs"):
        try:
            all_rows.extend(analyze_pdf(pdf_path))
        except Exception as e:
            all_rows.append(
                {
                    "document": pdf_path.name,
                    "page_number": -1,
                    "error": str(e),
                }
            )

    page_df = pd.DataFrame(all_rows)
    page_df.to_csv(out_dir / "page_metrics.csv", index=False)

    good_df = page_df[page_df.get("page_number", -1) > 0].copy()
    summary = []
    for doc_name, g in good_df.groupby("document"):
        summary.append(
            {
                "document": doc_name,
                "pages": int(g["page_number"].max()),
                "avg_char_count": float(g["char_count"].mean()),
                "avg_char_density": float(g["char_density"].mean()),
                "avg_whitespace_ratio": float(g["whitespace_ratio"].mean()),
                "avg_image_area_ratio": float(g["image_area_ratio"].mean()),
                "scanned_pages_ratio": float(g["scanned_likely"].mean()),
                "origin_type_guess": classify_origin(g),
                "layout_complexity_guess": classify_layout(g),
            }
        )

    summary_df = pd.DataFrame(summary).sort_values("document")
    summary_df.to_csv(out_dir / "document_summary.csv", index=False)

    # quick threshold draft for phase 0 notes
    thresholds = {
        "native_digital": {
            "avg_char_count_per_page": "> 100",
            "avg_image_area_ratio": "< 0.50",
        },
        "scanned_image": {
            "char_count_per_page": "< 30",
            "image_area_ratio": "> 0.50",
            "scanned_pages_ratio": ">= 0.85",
        },
        "mixed": {
            "description": "No origin type reaches 85% of pages",
        },
    }
    with open(out_dir / "threshold_draft.json", "w", encoding="utf-8") as f:
        json.dump(thresholds, f, indent=2)

    print(f"Saved: {out_dir/'page_metrics.csv'}")
    print(f"Saved: {out_dir/'document_summary.csv'}")
    print(f"Saved: {out_dir/'threshold_draft.json'}")


if __name__ == "__main__":
    main()
