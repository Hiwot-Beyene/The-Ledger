"""
Surya OCR integration for the escalation guard (Strategy B).
Supports 90+ languages including Amharic (am).
"""
from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any


def _render_page_png(pdf_path: Path, page_number: int, dpi: int = 150) -> bytes:
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        try:
            page = doc.load_page(page_number - 1)
            pix = page.get_pixmap(dpi=dpi)
            return pix.tobytes("png")
        finally:
            doc.close()
    except Exception:
        pass
    import pdfplumber
    with pdfplumber.open(str(pdf_path)) as pdf:
        page = pdf.pages[page_number - 1]
        image = page.to_image(resolution=dpi).original
        buf = BytesIO()
        image.save(buf, format="PNG")
        return buf.getvalue()


def _render_all_pages_png(pdf_path: Path, page_numbers: list[int], dpi: int) -> list[bytes]:
    """Render multiple pages with a single PDF open."""
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        try:
            return [
                doc.load_page(pno - 1).get_pixmap(dpi=dpi).tobytes("png")
                for pno in page_numbers
            ]
        finally:
            doc.close()
    except Exception:
        pass
    return [_render_page_png(pdf_path, pno, dpi) for pno in page_numbers]


def run_surya_ocr_on_pages(
    pdf_path: Path,
    page_numbers: list[int],
    lang_codes: list[str] | None = None,
    dpi: int = 150,
) -> list[list[dict[str, Any]]]:
    """
    Run Surya OCR on the given pages. Returns one list per page;
    each list contains dicts with 'text' and 'bbox' (x0, y0, x1, y1).
    No API key: runs locally. Uses Amharic (am) when in lang_codes.
    """
    try:
        from PIL import Image
        from surya.detection import DetectionPredictor
        from surya.foundation import FoundationPredictor
        from surya.recognition import RecognitionPredictor
    except ImportError as e:
        raise ImportError(
            "surya-ocr and Pillow are required for Surya OCR. Install with: pip install surya-ocr Pillow"
        ) from e

    if lang_codes is None or len(lang_codes) == 0:
        lang_codes = ["en"]

    raw_list = _render_all_pages_png(pdf_path, page_numbers, dpi)
    images = [Image.open(BytesIO(raw)).convert("RGB") for raw in raw_list]

    det_predictor = DetectionPredictor()
    foundation_predictor = FoundationPredictor()
    rec_predictor = RecognitionPredictor(foundation_predictor)
    predictions = rec_predictor(images, lang_codes=lang_codes, det_predictor=det_predictor)

    per_page: list[list[dict[str, Any]]] = []
    for page_preds in predictions:
        lines = []
        if not isinstance(page_preds, dict):
            page_preds = {"text_lines": []}
        for line in page_preds.get("text_lines", []):
            if not isinstance(line, dict):
                continue
            text = (line.get("text") or "").strip()
            bbox = line.get("bbox")
            if not text and not bbox:
                continue
            if bbox and len(bbox) >= 4:
                x0, y0, x1, y1 = float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
            else:
                x0, y0, x1, y1 = 0.0, 0.0, 0.0, 0.0
            lines.append({"text": text, "bbox": (x0, y0, x1, y1)})
        per_page.append(lines)
    return per_page
