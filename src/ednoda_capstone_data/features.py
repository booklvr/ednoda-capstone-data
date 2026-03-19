"""Lightweight sentence-level feature extraction helpers."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import re


CONTENT_POS_TAGS = {"NOUN", "PROPN", "VERB", "ADJ", "ADV"}
SURFACE_TOKEN_RE = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def surface_tokens(text: str) -> list[str]:
    return SURFACE_TOKEN_RE.findall(text or "")


def safe_divide(numerator: float, denominator: float) -> float | None:
    if not denominator:
        return None
    return float(numerator) / float(denominator)


def lexical_density_from_pos(pos_tags: list[str]) -> tuple[int, int, float | None]:
    alpha_count = len([tag for tag in pos_tags if tag])
    content_count = len([tag for tag in pos_tags if tag in CONTENT_POS_TAGS])
    return alpha_count, content_count, safe_divide(content_count, alpha_count)


def compute_surface_features(text: str) -> dict[str, float | int | None]:
    tokens = surface_tokens(text)
    token_count = len(tokens)
    unique_count = len({token.lower() for token in tokens})
    total_chars = sum(len(token) for token in tokens)
    return {
        "surface_token_count": token_count,
        "surface_unique_token_ratio": safe_divide(unique_count, token_count),
        "surface_avg_token_length": safe_divide(total_chars, token_count),
        "surface_char_count": len(text or ""),
    }


def compact_json(values: object) -> str:
    return json.dumps(values, ensure_ascii=False)


def count_selected_pos(pos_tags: list[str]) -> dict[str, int]:
    counts = Counter(pos_tags)
    return {
        "noun_count": counts.get("NOUN", 0) + counts.get("PROPN", 0),
        "verb_count": counts.get("VERB", 0),
        "adj_count": counts.get("ADJ", 0),
        "adv_count": counts.get("ADV", 0),
        "aux_count": counts.get("AUX", 0),
        "pron_count": counts.get("PRON", 0),
    }
