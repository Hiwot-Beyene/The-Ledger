"""
Vision extraction (Strategy C): VLM when API key set, else Tesseract OCR.
On VLM failure or no key → OCR. On OCR unavailable → raise (router falls back to Layout).
Tesseract used with script-appropriate lang (e.g. eng, amh+eng for Ethiopic).
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from io import BytesIO

try:
    import fitz
except Exception:
    fitz = None
import pdfplumber

from src.models import (
    BBox,
    DocumentProfile,
    ExtractedDocument,
    ExtractedMetadata,
    ExtractedPage,
    LDU,
    PageIndexNode,
    ProvenanceChain,
    StrategyName,
    TextBlock,
    content_hash_for_text,
)
from src.services.model_gateway import ModelGateway
from src.services.pricing import estimate_vision_cost_per_page, estimate_vision_run_cost
from src.strategies.base import ExtractionStrategy
from src.exceptions import BudgetApprovalRequired
from src.utils.checkpoint import (
    load_vision_checkpoint,
    save_vision_checkpoint,
    has_checkpoint,
    delete_vision_checkpoint,
)


VISION_JSON_PROMPT = """Extract all text from this page image. Return a single JSON object with this exact shape, no other text:
{"blocks": [{"text": "...", "x0": 0, "y0": 0, "x1": 0, "y1": 0}, ...]}
Use coordinates in points (72 points = 1 inch). One block per paragraph or logical segment. If no text, return {"blocks": []}."""

DPI = 150


def _default_dpi(rules: dict) -> int:
    return int(rules.get("vision", {}).get("dpi", DPI))


# ISO 639-1 (profile) -> Tesseract lang code (single). Multilingual = primary+eng.
_TESSERACT_LANG_MAP = {
    "am": "amh",
    "ti": "tir",
    "ar": "ara",
    "fa": "fas",
    "en": "eng",
}


def _get_tesseract_lang(profile: DocumentProfile, rules: dict) -> str:
    """Return Tesseract lang string from profile language (e.g. eng, amh+eng)."""
    code = (profile.language.code or "").strip().lower()
    default = (rules.get("vision", {}).get("default_ocr_lang") or "eng").strip().lower()
    conf = getattr(profile.language, "confidence", 1.0)
    # Unknown or missing language: use English so scanned docs get fast English OCR by default.
    if not code or code == "und":
        return "eng"
    if code and code != "und":
        tesseract = _TESSERACT_LANG_MAP.get(code) or (code if len(code) == 3 else "eng")
        if tesseract == "eng":
            return "eng"
        return f"{tesseract}+eng"
    if default == "amh":
        return "amh+eng"
    if default in ("ara", "tir", "fas"):
        return f"{default}+eng"
    return default if len(default) == 3 else "eng"


def _get_surya_lang_codes(profile: DocumentProfile, rules: dict) -> list[str]:
    code = (profile.language.code or "").strip().lower()
    if code and code != "und":
        return [code] if code != "en" else ["en"]
    default = (rules.get("vision", {}).get("default_ocr_lang") or "eng").strip().lower()
    if default == "amh":
        return ["am", "en"]
    return [default] if default != "eng" else ["en"]


def _tesseract_has_lang(lang: str) -> bool:
    """Return True if Tesseract has the given language (e.g. 'amh') installed."""
    try:
        import subprocess
        result = subprocess.run(
            ["tesseract", "--list-langs"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return False
        return lang.strip().lower() in (result.stdout or "").lower().split()
    except Exception:
        return False


def _tesseract_image_to_string(pytesseract, img, lang: str) -> str:
    """Run Tesseract with lang; on missing tessdata try primary lang then fallback to eng."""
    try:
        return (pytesseract.image_to_string(img, lang=lang) or "").strip()
    except pytesseract.TesseractError:
        if "+" in lang:
            primary = lang.split("+")[0].strip()
            try:
                return (pytesseract.image_to_string(img, lang=primary) or "").strip()
            except pytesseract.TesseractError:
                pass
        fallback = (pytesseract.image_to_string(img, lang="eng") or "").strip()
        logging.warning("Tesseract lang=%r failed (install tessdata for that language). Using eng.", lang)
        return fallback


def _build_base_doc_from_fitz(
    pdf_path: Path, profile: DocumentProfile, page_count: int
) -> ExtractedDocument:
    if fitz is None:
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = [
                ExtractedPage(
                    page_number=pno,
                    width=float(p.width),
                    height=float(p.height),
                    text_blocks=[],
                    ldu_ids=[],
                )
                for pno, p in enumerate(pdf.pages, start=1)
            ]
    else:
        doc = fitz.open(str(pdf_path))
        try:
            pages = [
                ExtractedPage(
                    page_number=pno,
                    width=doc.load_page(pno - 1).rect.width,
                    height=doc.load_page(pno - 1).rect.height,
                    text_blocks=[],
                    ldu_ids=[],
                )
                for pno in range(1, page_count + 1)
            ]
        finally:
            doc.close()

    return ExtractedDocument(
        doc_id=profile.doc_id,
        document_name=profile.document_name,
        pages=pages,
        metadata=ExtractedMetadata(
            source_strategy=StrategyName.C,
            confidence_score=0.65,
            strategy_sequence=[StrategyName.C],
        ),
        ldus=[],
        provenance_chains=[],
    )


def _build_ldus_provenance_and_index(
    extracted: ExtractedDocument, profile: DocumentProfile
) -> None:
    ldus: list[LDU] = []
    provenance_chains: list[ProvenanceChain] = []
    page_nodes: list[PageIndexNode] = []

    for page in extracted.pages:
        page_ldu_ids: list[str] = []
        block_nodes: list[PageIndexNode] = []
        for i, block in enumerate(page.text_blocks):
            content_hash = content_hash_for_text(block.text)
            chain = ProvenanceChain(
                document_name=profile.document_name,
                page_number=page.page_number,
                bbox=block.bbox,
                content_hash=content_hash,
            )
            provenance_chains.append(chain)
            ldu = LDU(
                id=f"ldu-{block.id}",
                text=block.text,
                content_hash=content_hash,
                parent_section=f"page_{page.page_number}",
                page_refs=[page.page_number],
                provenance_chain=[chain],
            )
            ldus.append(ldu)
            page_ldu_ids.append(ldu.id)
            block_nodes.append(
                PageIndexNode(
                    id=block.id,
                    node_type="text_block",
                    label=(block.text[:80] if block.text else None),
                    page_number=page.page_number,
                    bbox=block.bbox,
                    children=[],
                )
            )
        page.ldu_ids = page_ldu_ids
        page_nodes.append(
            PageIndexNode(
                id=f"page-{page.page_number}",
                node_type="page",
                label=f"Page {page.page_number}",
                page_number=page.page_number,
                bbox=BBox(x0=0.0, y0=0.0, x1=page.width, y1=page.height),
                children=block_nodes,
            )
        )

    for i in range(1, len(ldus)):
        if ldus[i].page_refs and ldus[i - 1].page_refs and ldus[i].page_refs[0] == ldus[i - 1].page_refs[0]:
            ldus[i].previous_chunk_id = ldus[i - 1].id
            ldus[i - 1].next_chunk_id = ldus[i].id

    extracted.ldus = ldus
    extracted.provenance_chains = provenance_chains
    extracted.page_index = PageIndexNode(
        id=f"doc-{profile.doc_id}",
        node_type="document",
        label=profile.document_name,
        children=page_nodes,
    )


def _parse_vlm_blocks(raw: str, page_width: float, page_height: float) -> list[dict]:
    raw = (raw or "").strip()
    to_parse: str | None = None
    code_block = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
    if code_block:
        to_parse = code_block.group(1).strip()
    if not to_parse:
        json_match = re.search(r"\{[\s\S]*\}", raw)
        to_parse = json_match.group(0) if json_match else None
    if not to_parse:
        return []
    try:
        data = json.loads(to_parse)
        blocks = data.get("blocks") if isinstance(data, dict) else []
        if not isinstance(blocks, list):
            return []
        out = []
        for b in blocks:
            if not isinstance(b, dict):
                continue
            text = (b.get("text") or "").strip()
            x0 = float(b.get("x0", 0))
            y0 = float(b.get("y0", 0))
            x1 = float(b.get("x1", page_width))
            y1 = float(b.get("y1", page_height))
            if x1 < x0:
                x1, x0 = x0, x1
            if y1 < y0:
                y1, y0 = y0, y1
            out.append({"text": text, "x0": x0, "y0": y0, "x1": x1, "y1": y1})
        return out
    except (json.JSONDecodeError, TypeError, ValueError):
        return []


class VisionExtractor(ExtractionStrategy):
    name = "C"

    @staticmethod
    def _render_page_png(pdf_path: Path, page_number: int, dpi: int = DPI) -> bytes:
        if fitz is not None:
            doc = fitz.open(str(pdf_path))
            try:
                page = doc.load_page(page_number - 1)
                pix = page.get_pixmap(dpi=dpi)
                return pix.tobytes("png")
            finally:
                doc.close()
        with pdfplumber.open(str(pdf_path)) as pdf:
            page = pdf.pages[page_number - 1]
            image = page.to_image(resolution=dpi).original
            buffer = BytesIO()
            image.save(buffer, format="PNG")
            return buffer.getvalue()

    def _ocr_extract(
        self, pdf_path: Path, profile: DocumentProfile, rules: dict, base_doc: ExtractedDocument, dpi: int
    ) -> tuple[ExtractedDocument, float, float]:
        """Tesseract OCR with profile/rules language. Uses amh+eng for Amharic (multilingual)."""
        try:
            from PIL import Image
            import pytesseract
        except ImportError as e:
            raise ImportError(
                "pytesseract and Pillow are required for OCR fallback. Install: pip install pytesseract Pillow; install Tesseract system binary."
            ) from e

        tesseract_lang = _get_tesseract_lang(profile, rules)
        if fitz is not None:
            doc = fitz.open(str(pdf_path))
            try:
                for page in base_doc.pages:
                    pix = doc.load_page(page.page_number - 1).get_pixmap(dpi=dpi)
                    img = Image.open(BytesIO(pix.tobytes("png"))).convert("RGB")
                    text = _tesseract_image_to_string(pytesseract, img, tesseract_lang)
                    page.text_blocks.append(
                        TextBlock(
                            id=f"p{page.page_number}-ocr",
                            text=text,
                            bbox=BBox(x0=0.0, y0=0.0, x1=page.width, y1=page.height),
                            reading_order=0,
                        )
                    )
            finally:
                doc.close()
        else:
            for page in base_doc.pages:
                png_bytes = self._render_page_png(pdf_path, page.page_number, dpi=dpi)
                img = Image.open(BytesIO(png_bytes)).convert("RGB")
                text = _tesseract_image_to_string(pytesseract, img, tesseract_lang)
                page.text_blocks.append(
                    TextBlock(
                        id=f"p{page.page_number}-ocr",
                        text=text,
                        bbox=BBox(x0=0.0, y0=0.0, x1=page.width, y1=page.height),
                        reading_order=0,
                    )
                )

        _build_ldus_provenance_and_index(base_doc, profile)
        # No API calls for local Tesseract OCR — cost is 0. Optional virtual cost from config for display.
        vision_cfg = rules.get("vision", {})
        virtual = float(vision_cfg.get("ocr_virtual_cost_per_page_usd", 0.0))
        cost = virtual * len(base_doc.pages) if virtual else 0.0
        return base_doc, 0.65, cost

    def _ensure_all_pages_filled(
        self,
        pdf_path: Path,
        profile: DocumentProfile,
        rules: dict,
        base_doc: ExtractedDocument,
        dpi: int,
    ) -> None:
        """Fill any page that has no text_blocks with OCR. Guarantees 100% page extraction per challenge."""
        empty = sorted(p.page_number for p in base_doc.pages if not p.text_blocks)
        for pno in empty:
            self._ocr_fill_pages(pdf_path, profile, rules, base_doc, dpi, pno, pno)

    def _ocr_fill_pages(
        self,
        pdf_path: Path,
        profile: DocumentProfile,
        rules: dict,
        base_doc: ExtractedDocument,
        dpi: int,
        start_1based: int,
        end_1based: int,
    ) -> None:
        """Fill base_doc.pages[start_1based-1:end_1based] with Tesseract OCR. Single PDF open when using fitz."""
        try:
            from PIL import Image
            import pytesseract
        except ImportError:
            return
        tesseract_lang = _get_tesseract_lang(profile, rules)
        if fitz is not None:
            doc = fitz.open(str(pdf_path))
            try:
                for pno in range(start_1based, end_1based + 1):
                    page = base_doc.pages[pno - 1]
                    pix = doc.load_page(pno - 1).get_pixmap(dpi=dpi)
                    img = Image.open(BytesIO(pix.tobytes("png"))).convert("RGB")
                    text = _tesseract_image_to_string(pytesseract, img, tesseract_lang)
                    page.text_blocks.append(
                        TextBlock(
                            id=f"p{page.page_number}-ocr",
                            text=text,
                            bbox=BBox(x0=0.0, y0=0.0, x1=page.width, y1=page.height),
                            reading_order=0,
                        )
                    )
            finally:
                doc.close()
        else:
            for pno in range(start_1based, end_1based + 1):
                page = base_doc.pages[pno - 1]
                png_bytes = self._render_page_png(pdf_path, pno, dpi=dpi)
                img = Image.open(BytesIO(png_bytes)).convert("RGB")
                text = _tesseract_image_to_string(pytesseract, img, tesseract_lang)
                page.text_blocks.append(
                    TextBlock(
                        id=f"p{page.page_number}-ocr",
                        text=text,
                        bbox=BBox(x0=0.0, y0=0.0, x1=page.width, y1=page.height),
                        reading_order=0,
                    )
                )

    def _surya_extract(
        self, pdf_path: Path, profile: DocumentProfile, rules: dict, base_doc: ExtractedDocument, dpi: int
    ) -> tuple[ExtractedDocument, float, float]:
        """Surya OCR with profile/rules language for non-Latin scripts."""
        from src.services.surya_ocr import run_surya_ocr_on_pages

        page_numbers = [p.page_number for p in base_doc.pages]
        lang_codes = _get_surya_lang_codes(profile, rules)
        per_page = run_surya_ocr_on_pages(pdf_path, page_numbers, lang_codes=lang_codes, dpi=dpi)
        scale = 72.0 / max(dpi, 1)
        for i, page in enumerate(base_doc.pages):
            lines = per_page[i] if i < len(per_page) else []
            if not lines:
                page.text_blocks.append(
                    TextBlock(
                        id=f"p{page.page_number}-surya0",
                        text="",
                        bbox=BBox(x0=0.0, y0=0.0, x1=page.width, y1=page.height),
                        reading_order=0,
                    )
                )
            for j, line in enumerate(lines):
                bbox = line.get("bbox") or (0.0, 0.0, 0.0, 0.0)
                if len(bbox) >= 4:
                    x0, y0, x1, y1 = float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
                    x0, y0, x1, y1 = x0 * scale, y0 * scale, x1 * scale, y1 * scale
                else:
                    x0, y0, x1, y1 = 0.0, 0.0, page.width, page.height
                page.text_blocks.append(
                    TextBlock(
                        id=f"p{page.page_number}-surya{j}",
                        text=line.get("text") or "",
                        bbox=BBox(x0=x0, y0=y0, x1=x1, y1=y1),
                        reading_order=j,
                    )
                )
        _build_ldus_provenance_and_index(base_doc, profile)
        vision_cfg = rules.get("vision", {})
        virtual = float(vision_cfg.get("ocr_virtual_cost_per_page_usd", 0.0))
        cost = virtual * len(base_doc.pages) if virtual else 0.0
        return base_doc, 0.65, cost

    def extract(self, pdf_path: Path, profile: DocumentProfile, rules: dict) -> tuple[ExtractedDocument, float, float]:
        vision_cfg = rules.get("vision", {})
        per_page_cost = float(vision_cfg.get("estimated_cost_per_page_usd", 0.02))
        runtime_model_cfg = rules.get("runtime_model", {})
        max_cost = float(runtime_model_cfg.get("max_vision_budget_usd", vision_cfg.get("max_cost_per_doc_usd", 2.0)))
        prefer_vlm = vision_cfg.get("prefer_vlm", True)
        dpi = _default_dpi(rules)

        if fitz is None:
            with pdfplumber.open(str(pdf_path)) as pdf:
                page_count = len(pdf.pages)
        else:
            doc = fitz.open(str(pdf_path))
            page_count = doc.page_count
            doc.close()

        base_doc = _build_base_doc_from_fitz(pdf_path, profile, page_count)
        gateway = ModelGateway(rules, runtime_config=runtime_model_cfg)
        selected_override = runtime_model_cfg.get("vision_override") if not runtime_model_cfg.get("auto_select", True) else None
        code = (profile.language.code or "").strip().lower()
        provider, model_name = gateway.select_vision_model(override=selected_override)
        has_vision_api = gateway.providers.get(provider) is not None
        paid_provider = gateway.is_paid_provider(provider)

        # Fast path: Latin/English or unknown script → Tesseract OCR only (efficient for scanned docs).
        prefer_ocr_latin = bool(vision_cfg.get("prefer_ocr_for_english_scanned", True))
        if (code in ("en", "", "und") or not code) and prefer_ocr_latin:
            return self._ocr_extract(pdf_path, profile, rules, base_doc, dpi)

        if has_vision_api and prefer_vlm:
            job_id = runtime_model_cfg.get("job_id")
            checkpoint_interval = max(1, int(vision_cfg.get("checkpoint_interval_pages", 5)))
            resume_from_checkpoint = bool(runtime_model_cfg.get("resume_from_checkpoint", False))
            require_approval = bool(
                vision_cfg.get("require_approval_over_budget", False)
                or runtime_model_cfg.get("require_approval_over_budget", False)
            )
            cost_per_page = estimate_vision_cost_per_page(
                provider, model_name, rules, runtime_model_cfg
            )
            max_vision_pages = int(vision_cfg.get("max_vision_pages", 10))
            effective_pages = (
                min(page_count, max(1, int(max_cost / cost_per_page)))
                if cost_per_page > 0
                else min(page_count, max_vision_pages)
            )
            estimated_cost = estimate_vision_run_cost(
                page_count, provider, model_name, rules, runtime_model_cfg
            )
            if (
                estimated_cost > max_cost
                and require_approval
                and not runtime_model_cfg.get("approve_run", False)
            ):
                raise BudgetApprovalRequired(estimated_cost, max_cost, page_count)

            total_cost = 0.0
            total_prompt_tokens = 0
            total_completion_tokens = 0
            start_page = 1
            doc_id = profile.doc_id
            if resume_from_checkpoint and has_checkpoint(doc_id):
                ck = load_vision_checkpoint(doc_id, None, str(pdf_path))
                if ck and len(ck.get("partial_doc", {}).get("pages", [])) == page_count:
                    base_doc = ExtractedDocument.model_validate(ck["partial_doc"])
                    total_cost = float(ck.get("total_cost_usd", 0))
                    total_prompt_tokens = int(ck.get("total_prompt_tokens", 0))
                    total_completion_tokens = int(ck.get("total_completion_tokens", 0))
                    start_page = int(ck.get("last_completed_page", 0)) + 1
                    if start_page > effective_pages:
                        start_page = effective_pages + 1

            vlm_ok = True
            last_completed = start_page - 1
            for pno in range(start_page, effective_pages + 1):
                if total_cost >= max_cost:
                    if job_id:
                        save_vision_checkpoint(
                            doc_id,
                            job_id,
                            last_completed,
                            total_cost,
                            total_prompt_tokens,
                            total_completion_tokens,
                            base_doc.model_dump(),
                            str(pdf_path),
                        )
                    break
                try:
                    image_bytes = self._render_page_png(pdf_path, pno, dpi=dpi)
                    result = gateway.generate_vision(
                        provider=provider,
                        model_name=model_name,
                        prompt=VISION_JSON_PROMPT,
                        image_bytes=image_bytes,
                    )
                    total_cost += result.estimated_cost_usd
                    total_prompt_tokens += result.prompt_tokens
                    total_completion_tokens += result.completion_tokens
                    page = base_doc.pages[pno - 1]
                    blocks = _parse_vlm_blocks(result.text, page.width, page.height)
                    if blocks:
                        for i, b in enumerate(blocks):
                            page.text_blocks.append(
                                TextBlock(
                                    id=f"p{pno}-vlm{i}",
                                    text=b["text"],
                                    bbox=BBox(x0=b["x0"], y0=b["y0"], x1=b["x1"], y1=b["y1"]),
                                    reading_order=i,
                                )
                            )
                        last_completed = pno
                        # Save checkpoint only every N pages (and when job_id set) to avoid slow I/O
                        if job_id and (
                            last_completed % checkpoint_interval == 0 or last_completed == effective_pages
                        ):
                            save_vision_checkpoint(
                                doc_id,
                                job_id,
                                last_completed,
                                total_cost,
                                total_prompt_tokens,
                                total_completion_tokens,
                                base_doc.model_dump(),
                                str(pdf_path),
                            )
                    else:
                        raw = (result.text or "").strip()
                        if raw and '"blocks"' in raw and "[]" in raw:
                            continue
                        vlm_ok = False
                        logging.warning(
                            "VLM returned empty/unparseable blocks (model=%s). Falling back to OCR.",
                            model_name,
                        )
                        break
                except Exception as e:
                    vlm_ok = False
                    logging.warning("VLM failed: %s. Falling back to OCR.", e)
                    break

            if vlm_ok and any(p.text_blocks for p in base_doc.pages):
                if last_completed < page_count:
                    self._ocr_fill_pages(pdf_path, profile, rules, base_doc, dpi, last_completed + 1, page_count)
                # Guarantee 100% extraction: fill any page that still has no content (challenge requirement).
                self._ensure_all_pages_filled(pdf_path, profile, rules, base_doc, dpi)
                _build_ldus_provenance_and_index(base_doc, profile)
                confidence = 0.65
                if total_cost >= max_cost:
                    confidence = max(0.45, confidence - 0.15)
                base_doc.metadata.confidence_score = confidence
                base_doc.metadata.prompt_tokens = total_prompt_tokens
                base_doc.metadata.completion_tokens = total_completion_tokens
                delete_vision_checkpoint(doc_id)
                return base_doc, confidence, min(total_cost, max_cost)

            for p in base_doc.pages:
                p.text_blocks.clear()
            logging.warning("VLM returned no text; falling back to OCR (Tesseract or Surya).")

        prefer_surya = bool(vision_cfg.get("prefer_surya_for_non_latin", False))
        default_lang = (vision_cfg.get("default_ocr_lang") or "eng").strip().lower()
        use_surya = prefer_surya and code not in ("", "en", "und")
        if use_surya:
            try:
                return self._surya_extract(pdf_path, profile, rules, base_doc, dpi)
            except Exception:
                for p in base_doc.pages:
                    p.text_blocks.clear()
        return self._ocr_extract(pdf_path, profile, rules, base_doc, dpi)
