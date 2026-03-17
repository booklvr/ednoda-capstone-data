"""Source-specific ingestion logic."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .io_utils import TABULAR_SUFFIXES, ensure_columns, read_tabular
from .normalize import normalize_cefr, normalize_text
from .schemas import (
    GRAMMAR_REFERENCE_COLUMNS,
    SENTENCES_COLUMNS,
    VOCABULARY_REFERENCE_COLUMNS,
)

TEXT_COLUMN_CANDIDATES = ["sentence", "text", "content", "example", "utterance"]
CEFR_COLUMN_CANDIDATES = ["cefr", "level", "difficulty", "cefr_level"]


def _choose_column(columns: list[str], candidates: list[str]) -> str | None:
    lowered = {c.lower(): c for c in columns}
    for key in lowered:
        for cand in candidates:
            if cand in key:
                return lowered[key]
    return None


def ingest_cefr_sp(raw_dir: Path) -> pd.DataFrame:
    rows: list[dict] = []
    tabular_files = [
        path
        for path in raw_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in TABULAR_SUFFIXES
    ]
    for path in tabular_files:
        try:
            df = read_tabular(path)
        except Exception:
            continue
        if df.empty:
            continue
        text_col = _choose_column(list(df.columns), TEXT_COLUMN_CANDIDATES)
        cefr_col = _choose_column(list(df.columns), CEFR_COLUMN_CANDIDATES)
        if not text_col:
            continue
        for idx, record in df.iterrows():
            text = normalize_text(record.get(text_col))
            if not text:
                continue
            cefr = normalize_cefr(record.get(cefr_col)) if cefr_col else None
            rows.append(
                {
                    "sentence_id": f"cefr_sp::{path.stem}::{idx}",
                    "source_name": "cefr-sp",
                    "source_dataset": path.name,
                    "source_record_id": str(idx),
                    "text": str(record.get(text_col)),
                    "text_normalized": text,
                    "language": "en",
                    "cefr_level": cefr,
                    "topic": pd.NA,
                    "license_id": "license_cefr_sp",
                    "metadata_json": json.dumps({"file": str(path.relative_to(raw_dir))}),
                }
            )
    return ensure_columns(pd.DataFrame(rows), SENTENCES_COLUMNS)


def ingest_cefrj_vocabulary(vocab_csv: Path, c1c2_csv: Path | None = None) -> pd.DataFrame:
    frames = []
    for path in [vocab_csv, c1c2_csv]:
        if path and path.exists():
            frames.append(pd.read_csv(path))
    if not frames:
        return ensure_columns(pd.DataFrame(), VOCABULARY_REFERENCE_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    rename_map = {
        "word": "headword",
        "lemma": "lemma",
        "pos": "pos",
        "cefr": "cefr_level",
        "level": "cefr_level",
        "example": "example",
    }
    combined = combined.rename(columns={k: v for k, v in rename_map.items() if k in combined.columns})
    combined["headword"] = combined.get("headword", pd.Series(dtype="object")).map(normalize_text)
    combined["lemma"] = combined.get("lemma", combined["headword"])
    combined["cefr_level"] = combined.get("cefr_level", pd.Series(dtype="object")).map(normalize_cefr)
    combined["vocab_id"] = [f"cefrj_vocab::{i}" for i in range(len(combined))]
    combined["source_name"] = "cefr-j"
    combined["source_dataset"] = "vocabulary"
    combined["frequency_band"] = combined.get("frequency_band", pd.NA)
    combined["notes"] = combined.get("notes", pd.NA)
    combined["license_id"] = "license_cefrj"
    return ensure_columns(combined, VOCABULARY_REFERENCE_COLUMNS)


def ingest_cefrj_grammar(grammar_csv: Path) -> pd.DataFrame:
    if not grammar_csv.exists():
        return ensure_columns(pd.DataFrame(), GRAMMAR_REFERENCE_COLUMNS)
    df = pd.read_csv(grammar_csv)
    rename_map = {
        "category": "grammar_label",
        "subcategory": "grammar_subcategory",
        "pattern": "pattern",
        "description": "description",
        "cefr": "cefr_level",
        "level": "cefr_level",
        "example": "example",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df["grammar_id"] = [f"cefrj_grammar::{i}" for i in range(len(df))]
    df["source_name"] = "cefr-j"
    df["source_dataset"] = "grammar"
    df["cefr_level"] = df.get("cefr_level", pd.Series(dtype="object")).map(normalize_cefr)
    df["notes"] = df.get("notes", pd.NA)
    df["license_id"] = "license_cefrj"
    return ensure_columns(df, GRAMMAR_REFERENCE_COLUMNS)


def ingest_ednoda_snapshot(snapshot_dir: Path) -> pd.DataFrame:
    files = sorted(snapshot_dir.glob("*.csv"))
    if not files:
        return ensure_columns(pd.DataFrame(), SENTENCES_COLUMNS)
    df = pd.read_csv(files[0])
    text_col = _choose_column(list(df.columns), TEXT_COLUMN_CANDIDATES) or "text"
    cefr_col = _choose_column(list(df.columns), CEFR_COLUMN_CANDIDATES)
    out = pd.DataFrame()
    out["sentence_id"] = [f"ednoda::{i}" for i in range(len(df))]
    out["source_name"] = "ednoda"
    out["source_dataset"] = files[0].name
    out["source_record_id"] = [str(i) for i in range(len(df))]
    out["text"] = df.get(text_col, pd.Series([""] * len(df)))
    out["text_normalized"] = out["text"].map(normalize_text)
    out["language"] = "en"
    out["cefr_level"] = df.get(cefr_col, pd.Series([None] * len(df))).map(normalize_cefr)
    out["topic"] = df.get("topic", pd.NA)
    out["license_id"] = "license_ednoda"
    out["metadata_json"] = "{}"
    return ensure_columns(out, SENTENCES_COLUMNS)
