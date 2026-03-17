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
    "BEGINNER": "A1",
    "ELEMENTARY": "A2",
    "INTERMEDIATE": "B1",
    "UPPER-INTERMEDIATE": "B2",
    "UPPER INTERMEDIATE": "B2",
    "ADVANCED": "C1",
}


def normalize_cefr(value: object) -> str | None:
    """Normalize diverse CEFR labels into canonical A1..C2."""
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL"}:
        return None
    text = re.sub(r"[^A-Z0-9\- ]", "", text)
    if text in CEFR_MAP:
        return CEFR_MAP[text]
    match = re.search(r"([ABC])\s*([12])", text)
    if match:
        return f"{match.group(1)}{match.group(2)}"
    return None


def normalize_text(value: object) -> str:
    """Normalize sentence text for deduping and matching."""
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()
