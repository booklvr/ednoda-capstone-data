"""Text and CEFR normalization utilities."""

from __future__ import annotations

import re
import unicodedata

CEFR_MAP = {
    "A1": "A1",
    "A2": "A2",
    "B1": "B1",
    "B2": "B2",
    "C1": "C1",
    "C2": "C2",
    "PRE-A1": "A1",
    "PRE A1": "A1",
    "A0": "A1",
}

CEFR_NUMERIC_MAP = {"A1": 1, "A2": 2, "B1": 3, "B2": 4, "C1": 5, "C2": 6}


def normalize_cefr(value: object) -> str | None:
    """Return canonical CEFR level in A1..C2 or None."""
    if value is None:
        return None
    raw = str(value).strip().upper()
    if not raw or raw in {"NAN", "NONE", "NULL"}:
        return None
    dotted_match = re.fullmatch(r"([ABC])\s*([12])\s*[.-]\s*\d+", raw)
    if dotted_match:
        level = f"{dotted_match.group(1)}{dotted_match.group(2)}"
        return level if level in CEFR_NUMERIC_MAP else None
    text = re.sub(r"[^A-Z0-9\- ]", "", raw)
    if text in CEFR_MAP:
        return CEFR_MAP[text]
    match = re.fullmatch(r"([ABC])\s*([12])", text)
    if match:
        level = f"{match.group(1)}{match.group(2)}"
        return level if level in CEFR_NUMERIC_MAP else None
    return None


def cefr_to_numeric(value: object) -> int | None:
    """Map CEFR level to integer in 1..6."""
    level = normalize_cefr(value)
    if not level:
        return None
    return CEFR_NUMERIC_MAP[level]


def normalize_text(value: object) -> str:
    """Normalize sentence text while keeping readability."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()
