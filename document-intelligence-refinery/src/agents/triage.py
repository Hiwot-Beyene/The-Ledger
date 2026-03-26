from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import pdfplumber

from src.agents.domain_classifier import DomainClassifier, create_domain_classifier
from src.models import (
    DocumentProfile,
    EstimatedExtractionCost,
    LanguageInfo,
    LayoutComplexity,
    OriginType,
    StrategyName,
    TriageSignals,
)
from src.utils.language import detect_language, detect_script_from_image
from src.utils.ledger import write_json
from src.utils.rules import load_rules


class TriageAgent:
    def __init__(self, rules: dict, domain_classifier: DomainClassifier | None = None):
        self.rules = rules
        if domain_classifier is not None:
            self.domain_classifier = domain_classifier
        else:
            classifier_name = (
                (rules or {}).get("triage", {}).get("domain_classifier", "keyword")
            )
            self.domain_classifier = create_domain_classifier(str(classifier_name))

    def _triage_cfg(self) -> dict:
        return self.rules.get("triage", {})

    @staticmethod
    def _safe_image_area(img: dict) -> float:
        x0 = img.get("x0", 0) or 0
        x1 = img.get("x1", 0) or 0
        top = img.get("top", 0) or 0
        bottom = img.get("bottom", 0) or 0
        return max(0.0, (x1 - x0)) * max(0.0, (bottom - top))

    def classify_origin_type(
        self,
        avg_char_count: float,
        avg_image_ratio: float,
        scanned_pages_ratio: float,
        form_fillable_ratio: float,
    ) -> OriginType:
        triage = self._triage_cfg()
        scanned_ratio_threshold = float(triage.get(
            "scanned_pages_ratio_threshold", 0.85))
        scanned_max_chars = float(triage.get("scanned_max_char_count", 30))
        scanned_min_image = float(triage.get("scanned_min_image_ratio", 0.5))
        native_min_chars = float(triage.get("native_min_char_count", 100))
        form_fillable_ratio_threshold = float(
            triage.get("form_fillable_ratio_threshold", 0.20)
        )

        if form_fillable_ratio >= form_fillable_ratio_threshold:
            return OriginType.FORM_FILLABLE

        if scanned_pages_ratio >= scanned_ratio_threshold or (
            avg_char_count <= scanned_max_chars and avg_image_ratio >= scanned_min_image
        ):
            return OriginType.SCANNED_IMAGE
        if scanned_pages_ratio >= 0.5 and avg_image_ratio >= scanned_min_image and avg_char_count < 100:
            return OriginType.SCANNED_IMAGE
        if avg_char_count >= native_min_chars and avg_image_ratio < scanned_min_image:
            return OriginType.NATIVE_DIGITAL
        return OriginType.MIXED

    def classify_layout_complexity(self, table_density: float, figure_density: float, column_variation: float) -> LayoutComplexity:
        triage = self._triage_cfg()
        table_heavy_density_threshold = float(
            triage.get("table_heavy_density_threshold", 0.15)
        )
        figure_heavy_density_threshold = float(
            triage.get("figure_heavy_density_threshold", 0.15)
        )
        multi_column_variation_threshold = float(
            triage.get("multi_column_variation_threshold", 0.35)
        )
        single_column_max_table_density = float(
            triage.get("single_column_max_table_density", 0.08)
        )
        single_column_max_variation = float(
            triage.get("single_column_max_variation", 0.20)
        )

        if table_density >= table_heavy_density_threshold:
            return LayoutComplexity.TABLE_HEAVY
        if figure_density >= figure_heavy_density_threshold:
            return LayoutComplexity.FIGURE_HEAVY
        if column_variation >= multi_column_variation_threshold:
            return LayoutComplexity.MULTI_COLUMN
        if table_density < single_column_max_table_density and column_variation < single_column_max_variation:
            return LayoutComplexity.SINGLE_COLUMN
        return LayoutComplexity.MIXED

    @staticmethod
    def select_strategy(origin: OriginType, layout: LayoutComplexity) -> StrategyName:
        if origin in {OriginType.SCANNED_IMAGE, OriginType.FORM_FILLABLE}:
            return StrategyName.C
        if layout in {LayoutComplexity.MULTI_COLUMN, LayoutComplexity.TABLE_HEAVY, LayoutComplexity.FIGURE_HEAVY, LayoutComplexity.MIXED}:
            return StrategyName.B
        return StrategyName.A

    def estimate_triage_confidence(
        self,
        avg_char_count: float,
        scanned_pages_ratio: float,
        form_fillable_ratio: float,
    ) -> float:
        triage = self._triage_cfg()
        native_min_chars = float(triage.get("native_min_char_count", 100))
        scanned_threshold = float(triage.get(
            "scanned_pages_ratio_threshold", 0.85))
        form_fillable_threshold = float(triage.get(
            "form_fillable_ratio_threshold", 0.20))

        chars_signal = min(
            max(avg_char_count / max(native_min_chars, 1.0), 0.0), 1.0)
        scanned_signal = min(
            max(scanned_pages_ratio / max(scanned_threshold, 1e-6), 0.0), 1.0)
        form_signal = min(
            max(form_fillable_ratio / max(form_fillable_threshold, 1e-6), 0.0), 1.0)
        return min(1.0, max(chars_signal, scanned_signal, form_signal))

    @staticmethod
    def estimate_cost(strategy: StrategyName) -> EstimatedExtractionCost:
        if strategy == StrategyName.A:
            return EstimatedExtractionCost.FAST_TEXT_SUFFICIENT
        if strategy == StrategyName.B:
            return EstimatedExtractionCost.NEEDS_LAYOUT_MODEL
        return EstimatedExtractionCost.NEEDS_VISION_MODEL

    def profile_document(self, pdf_path: str | Path, persist: bool = True, language_hint: str | None = None) -> DocumentProfile:
        pdf_path = Path(pdf_path)
        pages_data = []
        text_samples: list[str] = []

        with pdfplumber.open(str(pdf_path)) as pdf:
            triage_cfg = self._triage_cfg()
            scanned_max_chars = float(
                triage_cfg.get("scanned_max_char_count", 30))
            scanned_min_image = float(
                triage_cfg.get("scanned_min_image_ratio", 0.5))
            for page in pdf.pages:
                page_area = max(float(page.width * page.height), 1.0)
                chars = page.chars or []
                words = page.extract_words() or []
                text = " ".join(w.get("text", "") for w in words)
                text_samples.append(text)
                images = page.images or []
                lines = page.lines or []
                rects = page.rects or []

                char_count = len(chars)
                char_density = char_count / page_area
                image_area_ratio = min(sum(self._safe_image_area(
                    img) for img in images) / page_area, 1.0)

                char_bbox_area = 0.0
                for c in chars:
                    x0, x1 = c.get("x0", 0), c.get("x1", 0)
                    top, bottom = c.get("top", 0), c.get("bottom", 0)
                    char_bbox_area += max(0.0, (x1 - x0)) * \
                        max(0.0, (bottom - top))

                text_coverage = min(char_bbox_area / page_area, 1.0)
                whitespace_ratio = 1.0 - text_coverage
                scanned_likely = int(
                    char_count < scanned_max_chars and image_area_ratio > scanned_min_image)
                annots = page.annots or []
                form_like = int(len(annots) > 0)

                x_positions = [float(w.get("x0", 0)) for w in words]
                column_variation = 0.0
                if x_positions:
                    column_variation = (
                        max(x_positions) - min(x_positions)) / max(float(page.width), 1.0)

                pages_data.append(
                    {
                        "char_count": char_count,
                        "char_density": char_density,
                        "whitespace_ratio": whitespace_ratio,
                        "image_area_ratio": image_area_ratio,
                        "table_density": (len(lines) + len(rects)) / max(page_area, 1.0),
                        "figure_density": len(images) / max(page_area, 1.0),
                        "scanned_likely": scanned_likely,
                        "form_like": form_like,
                        "column_variation": column_variation,
                    }
                )

        avg_char_count = sum(p["char_count"]
                             for p in pages_data) / max(len(pages_data), 1)
        avg_char_density = sum(p["char_density"]
                               for p in pages_data) / max(len(pages_data), 1)
        avg_whitespace = sum(p["whitespace_ratio"]
                             for p in pages_data) / max(len(pages_data), 1)
        avg_img_ratio = sum(p["image_area_ratio"]
                            for p in pages_data) / max(len(pages_data), 1)
        table_density = sum(p["table_density"]
                            for p in pages_data) / max(len(pages_data), 1)
        figure_density = sum(p["figure_density"]
                             for p in pages_data) / max(len(pages_data), 1)
        scanned_ratio = sum(p["scanned_likely"]
                            for p in pages_data) / max(len(pages_data), 1)
        form_fillable_ratio = sum(p["form_like"]
                                  for p in pages_data) / max(len(pages_data), 1)
        column_variation = sum(p["column_variation"]
                               for p in pages_data) / max(len(pages_data), 1)

        if avg_char_count == 0 and scanned_ratio == 0:
            # Guard: zero-text and non-scanned signal should degrade to mixed low-confidence.
            scanned_ratio = 0.5

        origin = self.classify_origin_type(
            avg_char_count, avg_img_ratio, scanned_ratio, form_fillable_ratio)
        layout = self.classify_layout_complexity(
            table_density, figure_density, column_variation)
        selected_strategy = self.select_strategy(origin, layout)
        estimated_cost = self.estimate_cost(selected_strategy)
        triage_confidence = self.estimate_triage_confidence(
            avg_char_count=avg_char_count,
            scanned_pages_ratio=scanned_ratio,
            form_fillable_ratio=form_fillable_ratio,
        )

        text_blob = "\n".join(text_samples)
        lang_code, lang_conf = detect_language(text_blob)
        # User override: when a language is selected, use it when text is empty or detection is uncertain.
        if language_hint and (language_hint := (language_hint or "").strip().lower()):
            if not text_blob.strip() or lang_code == "und" or lang_conf < 0.6:
                lang_code = language_hint
                lang_conf = max(lang_conf, 0.8)
        # Scanned doc: use Tesseract OSD on first page to detect script (Latin → en, Ethiopic → am, etc.).
        # Run OSD for all scanned docs so Amharic/Ethiopic is detected when Tesseract supports it;
        # otherwise we may rely on text-based detection which can misclassify (e.g. "ru" for Amharic).
        use_osd = self._triage_cfg().get("detect_scanned_language_with_osd", True)
        if use_osd and origin == OriginType.SCANNED_IMAGE:
            try:
                with pdfplumber.open(str(pdf_path)) as pdf:
                    if pdf.pages:
                        first_page = pdf.pages[0]
                        pil_image = first_page.to_image(resolution=150).original
                        osd_code, osd_conf = detect_script_from_image(pil_image)
                        if osd_code != "und":
                            lang_code = osd_code
                            lang_conf = max(lang_conf, osd_conf)
            except Exception:
                pass
        # Scanned + "ru" from text is a known false positive for Amharic/Ethiopic; do not trust it.
        # Treat as "und" so default_scanned_language (e.g. "am") can apply when OSD does not detect script.
        if origin == OriginType.SCANNED_IMAGE and lang_code == "ru":
            geez_count = sum(1 for ch in text_blob if "\u1200" <= ch <= "\u137F")
            if geez_count >= 3:
                lang_code = "am"
                lang_conf = max(lang_conf, 0.55)
            else:
                lang_code = "und"
                lang_conf = min(lang_conf, 0.4)
        # Fallback default for scanned when we still have no signal (e.g. OSD failed or "ru" was overridden).
        default_scanned = (self._triage_cfg().get("default_scanned_language") or "und").strip().lower()
        if origin == OriginType.SCANNED_IMAGE and default_scanned != "und" and lang_code == "und" and lang_conf < 0.6:
            low_text = len(text_blob.strip()) < 80 or avg_char_count <= scanned_max_chars
            if low_text:
                lang_code = default_scanned
                lang_conf = max(lang_conf, 0.8)
        domain_hint = self.domain_classifier.classify(text_blob)

        doc_id = hashlib.sha1(
            str(pdf_path.resolve()).encode("utf-8")).hexdigest()[:16]
        profile = DocumentProfile(
            doc_id=doc_id,
            document_name=pdf_path.name,
            origin_type=origin,
            layout_complexity=layout,
            language=LanguageInfo(code=lang_code, confidence=lang_conf),
            domain_hint=domain_hint,
            estimated_extraction_cost=estimated_cost,
            triage_signals=TriageSignals(
                avg_char_density=avg_char_density,
                avg_whitespace_ratio=avg_whitespace,
                avg_image_area_ratio=avg_img_ratio,
                table_density=table_density,
                figure_density=figure_density,
            ),
            selected_strategy=selected_strategy,
            triage_confidence_score=triage_confidence,
        )

        if persist:
            target = Path(".refinery/profiles") / f"{doc_id}.json"
            write_json(target, profile.model_dump())

        return profile


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a DocumentProfile using the Triage Agent")
    parser.add_argument("--input", required=True, help="Path to input PDF")
    parser.add_argument(
        "--rules", default="rubric/extraction_rules.yaml", help="Rules file path")
    args = parser.parse_args()

    rules = load_rules(args.rules)
    agent = TriageAgent(rules)
    profile = agent.profile_document(args.input, persist=True)
    print(profile.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
