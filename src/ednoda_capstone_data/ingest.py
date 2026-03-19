"""Source-specific ingestion logic."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .io_utils import TABULAR_SUFFIXES, ensure_columns, read_tabular
from .normalize import cefr_to_numeric, normalize_cefr, normalize_text
from .schemas import GRAMMAR_REFERENCE_COLUMNS, SENTENCES_COLUMNS, VOCABULARY_REFERENCE_COLUMNS
from .sources import CEFRJ_C1C2_FILE, CEFRJ_GRAMMAR_FILE, CEFRJ_VOCAB_FILE

TEXT_COLUMN_CANDIDATES = ["sentence", "text", "content", "example", "utterance"]
CEFR_COLUMN_CANDIDATES = ["cefr", "level", "difficulty", "cefr_level"]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _choose_column(columns: list[str], candidates: list[str]) -> str | None:
    lowered = {c.lower(): c for c in columns}
    for key in lowered:
        for cand in candidates:
            if cand in key:
                return lowered[key]
    return None


def _infer_split(filename: str) -> str | None:
    f = filename.lower()
    if "train" in f:
        return "train"
    if "valid" in f or "dev" in f:
        return "validation"
    if "test" in f:
        return "test"
    return None


def ingest_cefr_sp(raw_dir: Path) -> pd.DataFrame:
    if not raw_dir.exists():
        print(f"[WARN] CEFR-SP raw directory not found: {raw_dir}")
        return ensure_columns(pd.DataFrame(), SENTENCES_COLUMNS)

    rows: list[dict] = []
    scanned_files: list[str] = []
    skipped_files: list[dict[str, str]] = []
    ts = _utc_now()
    tabular_files = [
        path for path in raw_dir.rglob("*") if path.is_file() and path.suffix.lower() in TABULAR_SUFFIXES
    ]
    if not tabular_files:
        print(f"[WARN] No tabular CEFR-SP files found under {raw_dir}")
        return ensure_columns(pd.DataFrame(), SENTENCES_COLUMNS)

    for path in tabular_files:
        scanned_files.append(str(path.relative_to(raw_dir)))
        try:
            df = read_tabular(path)
        except Exception as exc:
            skipped_files.append({"file": str(path.relative_to(raw_dir)), "reason": f"read_error: {exc}"})
            print(f"[WARN] Skipping CEFR-SP file {path}: read error")
            continue
        if df.empty:
            skipped_files.append({"file": str(path.relative_to(raw_dir)), "reason": "empty_table"})
            continue
        text_col = _choose_column(list(df.columns), TEXT_COLUMN_CANDIDATES)
        cefr_col = _choose_column(list(df.columns), CEFR_COLUMN_CANDIDATES)
        if not text_col:
            skipped_files.append({"file": str(path.relative_to(raw_dir)), "reason": "no_text_column_detected"})
            print(f"[WARN] Skipping CEFR-SP file {path}: no text-like column detected")
            continue
        split = _infer_split(path.name)
        for idx, record in df.iterrows():
            text_raw = record.get(text_col)
            text_norm = normalize_text(text_raw)
            if not text_norm:
                continue
            cefr = normalize_cefr(record.get(cefr_col)) if cefr_col else None
            rows.append(
                {
                    "record_id": f"cefr_sp::{path.stem}::{idx}",
                    "source_dataset": "cefr_sp",
                    "source_subdataset": path.name,
                    "source_record_id": str(idx),
                    "text": str(text_raw),
                    "text_normalized": text_norm,
                    "lang": "en",
                    "granularity": "sentence",
                    "cefr_level": cefr,
                    "cefr_numeric": cefr_to_numeric(cefr),
                    "difficulty_source": "gold",
                    "topic_hint": pd.NA,
                    "grammar_hint": pd.NA,
                    "grade_hint": pd.NA,
                    "region_hint": pd.NA,
                    "node_type": pd.NA,
                    "license_label": "Unknown (verify upstream)",
                    "license_url": pd.NA,
                    "is_publicly_redistributable": False,
                    "split": split,
                    "metadata_json": json.dumps(
                        {
                            "relative_file": str(path.relative_to(raw_dir)),
                            "scanned_files": scanned_files,
                            "skipped_files": skipped_files,
                        }
                    ),
                    "created_at_utc": ts,
                }
            )
    return ensure_columns(pd.DataFrame(rows), SENTENCES_COLUMNS)


def _normalize_vocab_record(record: pd.Series, idx: int, list_name: str, ts: str) -> dict:
    cefr = normalize_cefr(record.get("cefr") or record.get("level"))
    headword = normalize_text(record.get("headword") or record.get("word"))
    return {
        "record_id": f"cefrj_vocab::{list_name}::{idx}",
        "source_dataset": "cefrj",
        "source_record_id": str(idx),
        "headword": headword,
        "lemma": normalize_text(record.get("lemma") or headword),
        "surface_form": normalize_text(record.get("surface_form") or headword),
        "pos": normalize_text(record.get("pos")),
        "cefr_level": cefr,
        "cefr_numeric": cefr_to_numeric(cefr),
        "list_name": list_name,
        "license_label": "CEFR-J terms (verify)",
        "license_url": pd.NA,
        "metadata_json": json.dumps({"raw": {k: (None if pd.isna(v) else str(v)) for k, v in record.items()}}),
        "created_at_utc": ts,
    }


def ingest_cefrj_vocabulary(vocab_csv: Path, c1c2_csv: Path | None = None) -> pd.DataFrame:
    rows: list[dict] = []
    ts = _utc_now()
    files = [(vocab_csv, "cefrj_vocabulary_profile"), (c1c2_csv, "octanove_c1c2")]
    for path, list_name in files:
        if not path or not path.exists():
            if list_name == "cefrj_vocabulary_profile":
                print(f"[WARN] Required CEFR-J vocabulary file missing: {path}")
            continue
        df = pd.read_csv(path)
        expected = {"word", "cefr", "pos"} if path.name == CEFRJ_VOCAB_FILE else {"word", "cefr"}
        if expected.issubset(set(c.lower() for c in df.columns)):
            print(f"[INFO] Using explicit CEFR-J vocabulary mapping for {path.name}")
        else:
            print(f"[WARN] Falling back to heuristic CEFR-J vocabulary mapping for {path.name}")
        for idx, record in df.iterrows():
            rows.append(_normalize_vocab_record(record, idx, list_name, ts))
    return ensure_columns(pd.DataFrame(rows), VOCABULARY_REFERENCE_COLUMNS)


def ingest_cefrj_grammar(grammar_csv: Path) -> pd.DataFrame:
    if not grammar_csv.exists():
        print(f"[WARN] Required CEFR-J grammar file missing: {grammar_csv}")
        return ensure_columns(pd.DataFrame(), GRAMMAR_REFERENCE_COLUMNS)
    ts = _utc_now()
    rows: list[dict] = []
    df = pd.read_csv(grammar_csv)

    expected = {"category", "level", "pattern"} if grammar_csv.name == CEFRJ_GRAMMAR_FILE else set()
    if expected and expected.issubset(set(c.lower() for c in df.columns)):
        print(f"[INFO] Using explicit CEFR-J grammar mapping for {grammar_csv.name}")
    else:
        print(f"[WARN] Falling back to heuristic CEFR-J grammar mapping for {grammar_csv.name}")

    for idx, record in df.iterrows():
        cefr = normalize_cefr(record.get("cefr") or record.get("level"))
        grammar_item = normalize_text(record.get("grammar_item") or record.get("category") or record.get("pattern"))
        grammar_description = normalize_text(record.get("grammar_description") or record.get("description"))
        rows.append(
            {
                "record_id": f"cefrj_grammar::{idx}",
                "source_dataset": "cefrj",
                "source_record_id": str(idx),
                "grammar_item": grammar_item,
                "grammar_description": grammar_description,
                "cefr_level": cefr,
                "cefr_numeric": cefr_to_numeric(cefr),
                "framework": "CEFR-J",
                "tags_json": json.dumps(record.get("tags", [] if pd.isna(record.get("tags")) else [str(record.get("tags"))])),
                "examples_json": json.dumps(record.get("example", [] if pd.isna(record.get("example")) else [str(record.get("example"))])),
                "license_label": "CEFR-J terms (verify)",
                "license_url": pd.NA,
                "metadata_json": json.dumps({"raw": {k: (None if pd.isna(v) else str(v)) for k, v in record.items()}}),
                "created_at_utc": ts,
            }
        )
    return ensure_columns(pd.DataFrame(rows), GRAMMAR_REFERENCE_COLUMNS)


def ingest_ednoda_snapshot(snapshot_dir: Path) -> pd.DataFrame:
    if not snapshot_dir.exists():
        print(f"[WARN] Ednoda snapshot directory not found (optional): {snapshot_dir}")
        return ensure_columns(pd.DataFrame(), SENTENCES_COLUMNS)
    files = sorted(snapshot_dir.glob("*.csv"))
    if not files:
        print(f"[WARN] No Ednoda snapshot CSV found (optional): {snapshot_dir}")
        return ensure_columns(pd.DataFrame(), SENTENCES_COLUMNS)
    ts = _utc_now()
    rows: list[dict] = []
    df = pd.read_csv(files[0])
    text_col = _choose_column(list(df.columns), TEXT_COLUMN_CANDIDATES) or "text"
    cefr_col = _choose_column(list(df.columns), CEFR_COLUMN_CANDIDATES)
    for idx, record in df.iterrows():
        text_raw = record.get(text_col)
        text_norm = normalize_text(text_raw)
        if not text_norm:
            continue
        cefr = normalize_cefr(record.get(cefr_col)) if cefr_col else None
        node_type = normalize_text(record.get("node_type")) or pd.NA
        rows.append(
            {
                "record_id": f"ednoda_snapshot::{idx}",
                "source_dataset": "ednoda_snapshot",
                "source_subdataset": files[0].name,
                "source_record_id": str(idx),
                "text": str(text_raw),
                "text_normalized": text_norm,
                "lang": normalize_text(record.get("lang") or "en"),
                "granularity": normalize_text(record.get("granularity") or node_type or "sentence"),
                "cefr_level": cefr,
                "cefr_numeric": cefr_to_numeric(cefr),
                "difficulty_source": normalize_text(record.get("difficulty_source") or "unknown"),
                "topic_hint": record.get("topic_hint", record.get("topic", pd.NA)),
                "grammar_hint": record.get("grammar_hint", pd.NA),
                "grade_hint": record.get("grade_hint", pd.NA),
                "region_hint": record.get("region_hint", pd.NA),
                "node_type": node_type,
                "license_label": normalize_text(record.get("license_label") or "Internal/Private"),
                "license_url": record.get("license_url", pd.NA),
                "is_publicly_redistributable": bool(record.get("is_publicly_redistributable", False)),
                "split": normalize_text(record.get("split")) or _infer_split(files[0].name),
                "metadata_json": json.dumps({"raw": {k: (None if pd.isna(v) else str(v)) for k, v in record.items()}}),
                "created_at_utc": ts,
            }
        )
    return ensure_columns(pd.DataFrame(rows), SENTENCES_COLUMNS)
