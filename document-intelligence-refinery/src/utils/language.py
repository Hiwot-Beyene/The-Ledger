from __future__ import annotations

from typing import Any

from .rules import DEFAULT_RULES

try:
    from ftlangdetect import detect as ft_detect
except Exception:
    ft_detect = None

# Tesseract OSD script name -> our language code (ISO 639-1). Used for scanned-doc language detection.
_OSD_SCRIPT_TO_LANG: dict[str, str] = {
    "Latin": "en",
    "Cyrillic": "ru",
    "Arabic": "ar",
    "Devanagari": "hi",
    "Han": "zh",
    "Japanese": "ja",
    "Hangul": "ko",
    "Ethiopic": "am",
    "Thai": "th",
    "Hebrew": "he",
}


def detect_script_from_image(image: Any) -> tuple[str, float]:
    """
    Use Tesseract OSD (orientation/script detection) on a page image to infer language.
    Fast (no full OCR). Returns (lang_code, confidence). Falls back to ("und", 0.0) on failure.
    """
    try:
        import pytesseract
    except ImportError:
        return "und", 0.0
    try:
        # PSM 0 = OSD only; returns script (e.g. Latin, Ethiopic) without full OCR
        data = pytesseract.image_to_osd(image, config="--psm 0")
        if not isinstance(data, str):
            return "und", 0.0
        script = None
        for line in data.splitlines():
            line = line.strip()
            if line.lower().startswith("script:"):
                script = line.split(":", 1)[-1].strip().strip("'\"")
                break
        if script and script in _OSD_SCRIPT_TO_LANG:
            return _OSD_SCRIPT_TO_LANG[script], 0.75
        if script:
            return "und", 0.4
        return "und", 0.0
    except Exception:
        return "und", 0.0


def detect_language(text: str) -> tuple[str, float]:
    cleaned = (text or "").strip()
    if not cleaned:
        return "und", 0.0

    # Check for Ge'ez/Ethiopic script first so Amharic is never misclassified as e.g. "ru" by FastText.
    geez_chars = sum(1 for ch in cleaned if "\u1200" <= ch <= "\u137F")
    geez_ratio = geez_chars / max(len(cleaned), 1)
    if geez_ratio > 0.2:
        return "am", 0.55
    if geez_chars >= 3:
        return "am", 0.5

    if ft_detect is not None:
        try:
            result = ft_detect(text=cleaned, low_memory=True)
            return result.get("lang", "und"), float(result.get("score", 0.0))
        except Exception:
            pass

    latin_chars = sum(1 for ch in cleaned if ch.isascii() and ch.isalpha())
    latin_ratio = latin_chars / max(len(cleaned), 1)
    if latin_ratio > 0.3:
        return "en", 0.4

    return "und", 0.1
