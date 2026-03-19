#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import argparse

import pandas as pd

from ednoda_capstone_data.features import (
    compact_json,
    compute_surface_features,
    count_selected_pos,
    lexical_density_from_pos,
    utc_now,
)


OUTPUT_COLUMNS = [
    "record_id",
    "source_dataset",
    "source_subdataset",
    "cefr_level",
    "cefr_numeric",
    "split",
    "text_normalized",
    "surface_char_count",
    "surface_token_count",
    "surface_unique_token_ratio",
    "surface_avg_token_length",
    "alpha_token_count",
    "content_word_count",
    "lexical_density",
    "noun_count",
    "verb_count",
    "adj_count",
    "adv_count",
    "aux_count",
    "pron_count",
    "readability_flesch_reading_ease",
    "readability_flesch_kincaid_grade",
    "readability_gunning_fog",
    "readability_smog_index",
    "readability_dale_chall",
    "readability_automated_readability_index",
    "root_token",
    "root_pos",
    "pos_tags_json",
    "lemmas_json",
    "dep_tags_json",
    "metadata_json",
    "created_at_utc",
]


def _load_dependencies() -> tuple[object, object]:
    try:
        import spacy
    except ImportError as exc:
        raise SystemExit(
            "spaCy is not installed. Install the optional NLP stack with "
            "`python -m pip install '.[nlp]'` or `python -m pip install spacy textstat`, "
            "then run `python -m spacy download en_core_web_sm`."
        ) from exc

    try:
        import nltk
        nltk.download = lambda *args, **kwargs: True
    except ImportError:
        pass

    try:
        from textstat import textstat
    except ImportError as exc:
        raise SystemExit(
            "textstat is not installed. Install the optional NLP stack with "
            "`python -m pip install '.[nlp]'` or `python -m pip install spacy textstat`."
        ) from exc

    try:
        nlp = spacy.load("en_core_web_sm", disable=["ner"])
    except Exception as exc:
        raise SystemExit(
            "spaCy model `en_core_web_sm` is not installed. Run "
            "`python -m spacy download en_core_web_sm` and try again."
        ) from exc

    return nlp, textstat


def _safe_metric(fn, text: str) -> float | None:
    try:
        value = fn(text)
    except Exception:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Create sentence-level NLP and readability features.")
    parser.add_argument("--input", default="data/processed/sentences.parquet")
    parser.add_argument("--output", default="data/processed/sentence_features.parquet")
    args = parser.parse_args()

    nlp, textstat = _load_dependencies()

    sentences = pd.read_parquet(args.input)
    created_at = utc_now()
    rows: list[dict] = []

    texts = sentences["text_normalized"].fillna("").astype(str).tolist()
    for row, doc in zip(sentences.to_dict(orient="records"), nlp.pipe(texts, batch_size=256), strict=False):
        alpha_tokens = [token for token in doc if token.is_alpha]
        pos_tags = [token.pos_ for token in alpha_tokens]
        lemmas = [token.lemma_ for token in alpha_tokens]
        dep_tags = [token.dep_ for token in alpha_tokens]
        alpha_count, content_count, lexical_density = lexical_density_from_pos(pos_tags)
        pos_counts = count_selected_pos(pos_tags)
        surface = compute_surface_features(row["text_normalized"])
        roots = [token for token in doc if token.dep_ == "ROOT"]
        root = roots[0] if roots else None

        text = row["text_normalized"]
        rows.append(
            {
                "record_id": row["record_id"],
                "source_dataset": row["source_dataset"],
                "source_subdataset": row["source_subdataset"],
                "cefr_level": row["cefr_level"],
                "cefr_numeric": row["cefr_numeric"],
                "split": row["split"],
                "text_normalized": text,
                **surface,
                "alpha_token_count": alpha_count,
                "content_word_count": content_count,
                "lexical_density": lexical_density,
                **pos_counts,
                "readability_flesch_reading_ease": _safe_metric(textstat.flesch_reading_ease, text),
                "readability_flesch_kincaid_grade": _safe_metric(textstat.flesch_kincaid_grade, text),
                "readability_gunning_fog": _safe_metric(textstat.gunning_fog, text),
                "readability_smog_index": _safe_metric(textstat.smog_index, text),
                "readability_dale_chall": _safe_metric(textstat.dale_chall_readability_score, text),
                "readability_automated_readability_index": _safe_metric(textstat.automated_readability_index, text),
                "root_token": root.text if root is not None else None,
                "root_pos": root.pos_ if root is not None else None,
                "pos_tags_json": compact_json(pos_tags),
                "lemmas_json": compact_json(lemmas),
                "dep_tags_json": compact_json(dep_tags),
                "metadata_json": compact_json(
                    {
                        "feature_version": "day1_nlp_v1",
                        "spacy_model": "en_core_web_sm",
                        "uses_textstat": True,
                        "idea_density_note": "No true CPIDR/ACU score yet; lexical_density is included, but idea density should be treated as future work.",
                    }
                ),
                "created_at_utc": created_at,
            }
        )

    features = pd.DataFrame(rows)
    for col in OUTPUT_COLUMNS:
        if col not in features.columns:
            features[col] = pd.NA
    features = features[OUTPUT_COLUMNS]

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    features.to_parquet(output_path, index=False)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
