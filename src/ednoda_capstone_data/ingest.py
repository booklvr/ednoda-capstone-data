"""Source-specific ingestion logic."""

from __future__ import annotations

import json
import math
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


def _numeric_level_to_cefr(value: object) -> str | None:
    try:
        numeric = int(float(value))
    except (TypeError, ValueError):
        return None
    mapping = {1: "A1", 2: "A2", 3: "B1", 4: "B2", 5: "C1", 6: "C2"}
    return mapping.get(numeric)


def _cefr_sp_headerless_frame(path: Path) -> pd.DataFrame | None:
    if path.suffix.lower() not in {".txt", ".tsv"}:
        return None
    try:
        df = pd.read_csv(path, sep="\t", header=None)
    except Exception:
        return None
    if df.shape[1] < 3:
        return None
    return df.iloc[:, :3].rename(columns={0: "sentence", 1: "label_a", 2: "label_b"})


def _resolve_cefr_sp_label(record: pd.Series) -> tuple[str | None, dict[str, int | None]]:
    label_a = _numeric_level_to_cefr(record.get("label_a"))
    label_b = _numeric_level_to_cefr(record.get("label_b"))
    numeric_a = cefr_to_numeric(label_a)
    numeric_b = cefr_to_numeric(label_b)

    if numeric_a is not None and numeric_b is not None:
        resolved_numeric = math.floor(((numeric_a + numeric_b) / 2) + 0.5)
        resolved = _numeric_level_to_cefr(resolved_numeric)
    else:
        resolved = label_a or label_b

    return resolved, {
        "label_a_numeric": numeric_a,
        "label_b_numeric": numeric_b,
        "resolved_numeric": cefr_to_numeric(resolved),
    }


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
        cefr_sp_headerless = None
        if not text_col:
            cefr_sp_headerless = _cefr_sp_headerless_frame(path)
            if cefr_sp_headerless is not None:
                df = cefr_sp_headerless
                text_col = "sentence"
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
            metadata = {
                "relative_file": str(path.relative_to(raw_dir)),
                "scanned_files": scanned_files,
                "skipped_files": skipped_files,
            }
            if cefr_sp_headerless is not None:
                cefr, label_metadata = _resolve_cefr_sp_label(record)
                metadata.update(label_metadata)
            else:
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
                    "metadata_json": json.dumps(metadata),
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
        normalized = {c.lower(): c for c in df.columns}

        vocab_col = normalized.get("headword") or normalized.get("word")
        pos_col = normalized.get("pos")
        cefr_col = normalized.get("cefr") or normalized.get("level")
        lemma_col = normalized.get("lemma")
        if vocab_col and pos_col and cefr_col:
            print(f"[INFO] Using explicit CEFR-J vocabulary mapping for {path.name}")
            for idx, record in df.iterrows():
                metadata = {
                    "core_inventory_1": record.get(normalized.get("coreinventory 1")),
                    "core_inventory_2": record.get(normalized.get("coreinventory 2")),
                    "threshold": record.get(normalized.get("threshold")),
                    "notes": record.get(normalized.get("notes")),
                }
                rows.append(
                    {
                        "record_id": f"cefrj_vocab::{list_name}::{idx}",
                        "source_dataset": "cefrj",
                        "source_record_id": str(idx),
                        "headword": normalize_text(record.get(vocab_col)),
                        "lemma": normalize_text(record.get(lemma_col) or record.get(vocab_col)),
                        "surface_form": normalize_text(record.get(vocab_col)),
                        "pos": normalize_text(record.get(pos_col)),
                        "cefr_level": normalize_cefr(record.get(cefr_col)),
                        "cefr_numeric": cefr_to_numeric(record.get(cefr_col)),
                        "list_name": list_name,
                        "license_label": "CEFR-J terms (verify)",
                        "license_url": pd.NA,
                        "metadata_json": json.dumps(
                            {"raw": {k: (None if pd.isna(v) else str(v)) for k, v in metadata.items() if v is not None}}
                        ),
                        "created_at_utc": ts,
                    }
                )
            continue

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

    normalized = {c.lower(): c for c in df.columns}
    explicit_cols = {"id", "shorthand code", "grammatical item", "sentence type", "cefr-j level"}
    legacy_cols = {"category", "level", "pattern"}
    if explicit_cols.issubset(normalized):
        print(f"[INFO] Using explicit CEFR-J grammar mapping for {grammar_csv.name}")
        for idx, record in df.iterrows():
            cefr = normalize_cefr(record.get(normalized["cefr-j level"]) or record.get(normalized.get("freq*disp")))
            tags = [
                normalize_text(record.get(normalized.get("shorthand code"))),
                normalize_text(record.get(normalized.get("sentence type"))),
            ]
            tags = [tag for tag in tags if tag]
            examples = [normalize_text(record.get(normalized.get("grammatical item")))]
            examples = [example for example in examples if example]
            metadata = {
                "cefr_j_level_raw": record.get(normalized.get("cefr-j level")),
                "freq_disp": record.get(normalized.get("freq*disp")),
                "core_inventory": record.get(normalized.get("core inventory")),
                "egp": record.get(normalized.get("egp")),
                "gselo": record.get(normalized.get("gselo")),
                "notes": record.get(normalized.get("notes")),
                "shorthand_code": record.get(normalized.get("shorthand code")),
            }
            rows.append(
                {
                    "record_id": f"cefrj_grammar::{idx}",
                    "source_dataset": "cefrj",
                    "source_record_id": str(record.get(normalized["id"])),
                    "grammar_item": normalize_text(record.get(normalized["grammatical item"])),
                    "grammar_description": normalize_text(record.get(normalized.get("sentence type"))),
                    "cefr_level": cefr,
                    "cefr_numeric": cefr_to_numeric(cefr),
                    "framework": "CEFR-J",
                    "tags_json": json.dumps(tags),
                    "examples_json": json.dumps(examples),
                    "license_label": "CEFR-J terms (verify)",
                    "license_url": pd.NA,
                    "metadata_json": json.dumps(
                        {"raw": {k: (None if pd.isna(v) else str(v)) for k, v in metadata.items() if v is not None}},
                        ensure_ascii=False,
                    ),
                    "created_at_utc": ts,
                }
            )
        return ensure_columns(pd.DataFrame(rows), GRAMMAR_REFERENCE_COLUMNS)

    if legacy_cols.issubset(normalized):
        print(f"[INFO] Using explicit CEFR-J grammar mapping for legacy-format {grammar_csv.name}")
        for idx, record in df.iterrows():
            cefr = normalize_cefr(record.get(normalized["level"]))
            tags = [normalize_text(record.get(normalized["category"]))]
            tags = [tag for tag in tags if tag]
            examples = [normalize_text(record.get(normalized.get("example")))]
            examples = [example for example in examples if example]
            metadata = {
                "category": record.get(normalized.get("category")),
                "description": record.get(normalized.get("description")),
            }
            rows.append(
                {
                    "record_id": f"cefrj_grammar::{idx}",
                    "source_dataset": "cefrj",
                    "source_record_id": str(idx),
                    "grammar_item": normalize_text(record.get(normalized["pattern"])),
                    "grammar_description": normalize_text(record.get(normalized.get("description"))),
                    "cefr_level": cefr,
                    "cefr_numeric": cefr_to_numeric(cefr),
                    "framework": "CEFR-J",
                    "tags_json": json.dumps(tags),
                    "examples_json": json.dumps(examples),
                    "license_label": "CEFR-J terms (verify)",
                    "license_url": pd.NA,
                    "metadata_json": json.dumps({"raw": {k: (None if pd.isna(v) else str(v)) for k, v in metadata.items() if v is not None}}),
                    "created_at_utc": ts,
                }
            )
        return ensure_columns(pd.DataFrame(rows), GRAMMAR_REFERENCE_COLUMNS)

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
    lowered = {c.lower(): c for c in df.columns}
    education_node_id_col = lowered.get("education_node_id") or lowered.get("node_id") or lowered.get("id")
    textbook_col = lowered.get("textbook") or lowered.get("textbook_name")
    unit_col = lowered.get("unit") or lowered.get("unit_name") or lowered.get("textbook_unit")
    region_col = lowered.get("region") or lowered.get("region_hint")
    grade_col = lowered.get("grades") or lowered.get("grade") or lowered.get("grade_hint")
    topic_col = lowered.get("topic_hint") or lowered.get("topic")
    grammar_col = lowered.get("grammar_hint")
    analysis_col = lowered.get("analysis_json") or lowered.get("analysis")
    lang_col = lowered.get("lang")
    split_col = lowered.get("split")
    license_label_col = lowered.get("license_label")
    license_url_col = lowered.get("license_url")
    redistributable_col = lowered.get("is_publicly_redistributable")
    for idx, record in df.iterrows():
        text_raw = record.get(text_col)
        text_norm = normalize_text(text_raw)
        if not text_norm:
            continue
        cefr = normalize_cefr(record.get(cefr_col)) if cefr_col else None
        node_type = normalize_text(record.get("node_type")) or pd.NA
        metadata = {k: (None if pd.isna(v) else str(v)) for k, v in record.items()}
        rows.append(
            {
                "record_id": f"ednoda_snapshot::{record.get(education_node_id_col) if education_node_id_col else idx}",
                "source_dataset": "ednoda_snapshot",
                "source_subdataset": files[0].name,
                "source_record_id": str(record.get(education_node_id_col) if education_node_id_col else idx),
                "text": str(text_raw),
                "text_normalized": text_norm,
                "lang": normalize_text(record.get(lang_col) or "en"),
                "granularity": normalize_text(record.get("granularity") or node_type or "sentence"),
                "cefr_level": cefr,
                "cefr_numeric": cefr_to_numeric(cefr),
                "difficulty_source": normalize_text(record.get("difficulty_source") or "unknown"),
                "topic_hint": record.get(topic_col, pd.NA) if topic_col else pd.NA,
                "grammar_hint": record.get(grammar_col, pd.NA) if grammar_col else pd.NA,
                "grade_hint": record.get(grade_col, pd.NA) if grade_col else pd.NA,
                "region_hint": record.get(region_col, pd.NA) if region_col else pd.NA,
                "node_type": node_type,
                "license_label": normalize_text(record.get(license_label_col) or "Internal/Private"),
                "license_url": record.get(license_url_col, pd.NA) if license_url_col else pd.NA,
                "is_publicly_redistributable": bool(record.get(redistributable_col, False)) if redistributable_col else False,
                "split": normalize_text(record.get(split_col)) or _infer_split(files[0].name),
                "analysis_json": record.get(analysis_col, pd.NA) if analysis_col else pd.NA,
                "metadata_json": json.dumps(
                    {
                        "raw": metadata,
                        "textbook": record.get(textbook_col, None) if textbook_col else None,
                        "unit": record.get(unit_col, None) if unit_col else None,
                    },
                    ensure_ascii=False,
                ),
                "created_at_utc": ts,
            }
        )
    return ensure_columns(pd.DataFrame(rows), SENTENCES_COLUMNS)
