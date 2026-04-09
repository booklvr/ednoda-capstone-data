#!/usr/bin/env python
from __future__ import annotations

"""Prepare a cleaned Ednoda snapshot CSV from production table exports.

Expected inputs are CSV exports from:
- education_nodes
- node_analyses (optional)

This script does not connect to production directly. It transforms local CSV exports
into a single snapshot under data/raw/ednoda_snapshot/ that can be ingested by:
    python scripts/ingest_ednoda_snapshot.py
"""

import argparse
import json
from pathlib import Path

import pandas as pd


ATOMIC_NODE_TYPES = {"vocab", "expression", "question", "phonics"}


import csv

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    
    # Use standard csv module which handles pgAdmin's custom escapechar="'" beautifully
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, quotechar='"', escapechar="'")
        data = list(reader)
        
    if not data:
        return pd.DataFrame()
        
    df = pd.DataFrame(data[1:], columns=data[0])
    # Empty CSV fields become empty strings natively, covert them to NA for pandas
    return df.replace("", pd.NA)


def _normalize_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = " ".join(str(value).split())
    return text if text else None


def _latest_node_analysis(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["education_node_id"])

    normalized = {c.lower(): c for c in df.columns}
    node_id_col = normalized.get("education_node_id") or normalized.get("educationnodeid")
    if not node_id_col:
        raise ValueError("node_analyses export must include education_node_id")

    status_col = normalized.get("status")
    processed_col = normalized.get("processed_at")
    requested_col = normalized.get("requested_at")

    work = df.copy()
    if status_col:
        work = work[work[status_col].astype(str).str.lower() == "completed"].copy()

    if processed_col:
        work["_sort_ts"] = pd.to_datetime(work[processed_col], errors="coerce")
    elif requested_col:
        work["_sort_ts"] = pd.to_datetime(work[requested_col], errors="coerce")
    else:
        work["_sort_ts"] = pd.NaT

    work = work.sort_values(by=[node_id_col, "_sort_ts"], ascending=[True, False])
    latest = work.drop_duplicates(subset=[node_id_col], keep="first").copy()

    keep_cols = [
        node_id_col,
        normalized.get("status"),
        normalized.get("language_code"),
        normalized.get("analysis_version"),
        normalized.get("token_count"),
        normalized.get("sentence_count"),
        normalized.get("source_text_hash"),
        normalized.get("processed_at"),
        normalized.get("analysis"),
    ]
    keep_cols = [c for c in keep_cols if c is not None]
    latest = latest[keep_cols].copy()
    latest = latest.rename(columns={node_id_col: "education_node_id"})
    latest["education_node_id"] = latest["education_node_id"].astype("Int64")

    # Stable output names
    rename = {
        normalized.get("status"): "analysis_status",
        normalized.get("language_code"): "analysis_language_code",
        normalized.get("analysis_version"): "analysis_version",
        normalized.get("token_count"): "token_count",
        normalized.get("sentence_count"): "sentence_count",
        normalized.get("source_text_hash"): "source_text_hash",
        normalized.get("processed_at"): "analysis_processed_at",
        normalized.get("analysis"): "analysis_json",
    }
    rename = {k: v for k, v in rename.items() if k is not None}
    return latest.rename(columns=rename)


def _aggregate_node_grades(
    textbooks_df: pd.DataFrame,
    textbook_nodes_df: pd.DataFrame,
    lessons_df: pd.DataFrame,
    lesson_nodes_df: pd.DataFrame
) -> pd.DataFrame:
    """Extract and aggregate unique grades for each education_node_id."""
    grade_records = []

    # Merge textbooks
    if not textbooks_df.empty and not textbook_nodes_df.empty:
        tb = textbooks_df.copy()
        tbn = textbook_nodes_df.copy()
        
        tb_cols = {str(c).lower(): c for c in tb.columns}
        tbn_cols = {str(c).lower(): c for c in tbn.columns}
        
        if tb_cols.get("deleted_at"):
            tb = tb[tb[tb_cols["deleted_at"]].isna()].copy()
        if tbn_cols.get("deleted_at"):
            tbn = tbn[tbn[tbn_cols["deleted_at"]].isna()].copy()
            
        tb_id = tb_cols.get("id") or tb_cols.get("textbookid")
        tb_grade = tb_cols.get("grade")
        tbn_tb_id = tbn_cols.get("textbook_id") or tbn_cols.get("textbookid")
        tbn_en_id = tbn_cols.get("education_node_id") or tbn_cols.get("educationnodeid")
        
        if tb_id and tb_grade and tbn_tb_id and tbn_en_id:
            merged = tbn[[tbn_en_id, tbn_tb_id]].merge(
                tb[[tb_id, tb_grade]],
                left_on=tbn_tb_id,
                right_on=tb_id,
                how="inner"
            )
            merged = merged[[tbn_en_id, tb_grade]].rename(
                columns={tbn_en_id: "education_node_id", tb_grade: "grade"}
            )
            grade_records.append(merged)

    # Merge lessons
    if not lessons_df.empty and not lesson_nodes_df.empty:
        les = lessons_df.copy()
        lesn = lesson_nodes_df.copy()
        
        les_cols = {str(c).lower(): c for c in les.columns}
        lesn_cols = {str(c).lower(): c for c in lesn.columns}
        
        if les_cols.get("deleted_at"):
            les = les[les[les_cols["deleted_at"]].isna()].copy()
        if lesn_cols.get("deleted_at"):
            lesn = lesn[lesn[lesn_cols["deleted_at"]].isna()].copy()
            
        les_id = les_cols.get("id") or les_cols.get("lessonid")
        les_grade = les_cols.get("grade")
        lesn_les_id = lesn_cols.get("lesson_id") or lesn_cols.get("lessonid")
        lesn_en_id = lesn_cols.get("education_node_id") or lesn_cols.get("educationnodeid")
        
        if les_id and les_grade and lesn_les_id and lesn_en_id:
            merged = lesn[[lesn_en_id, lesn_les_id]].merge(
                les[[les_id, les_grade]],
                left_on=lesn_les_id,
                right_on=les_id,
                how="inner"
            )
            merged = merged[[lesn_en_id, les_grade]].rename(
                columns={lesn_en_id: "education_node_id", les_grade: "grade"}
            )
            grade_records.append(merged)
            
    if not grade_records:
        return pd.DataFrame(columns=["education_node_id", "grades"])
        
    all_grades = pd.concat(grade_records, ignore_index=True)
    all_grades = all_grades[all_grades["grade"].notna() & (all_grades["grade"].astype(str).str.strip() != "")]
    
    if all_grades.empty:
        return pd.DataFrame(columns=["education_node_id", "grades"])
    
    # group by education_node_id and make comma-separated string of unique sorted grades
    grouped = all_grades.groupby("education_node_id")["grade"].apply(
        lambda x: ", ".join(sorted(x.astype(str).unique()))
    ).reset_index()
    
    grouped.rename(columns={"grade": "grades"}, inplace=True)
    grouped["education_node_id"] = grouped["education_node_id"].astype("Int64")
    return grouped


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--education-nodes-csv",
        type=Path,
        default=Path("data/raw/ednoda_snapshot_exports/education_nodes.csv"),
        help="CSV export of education_nodes table",
    )
    parser.add_argument(
        "--node-analyses-csv",
        type=Path,
        default=Path("data/raw/ednoda_snapshot_exports/node_analyses.csv"),
        help="CSV export of node_analyses table (optional)",
    )
    parser.add_argument(
        "--textbooks-csv",
        type=Path,
        default=Path("data/raw/ednoda_snapshot_exports/textbooks.csv"),
        help="CSV export of textbooks table (optional, for grades)",
    )
    parser.add_argument(
        "--textbook-nodes-csv",
        type=Path,
        default=Path("data/raw/ednoda_snapshot_exports/textbook_nodes.csv"),
        help="CSV export of textbook_nodes table (optional, for grades)",
    )
    parser.add_argument(
        "--lessons-csv",
        type=Path,
        default=Path("data/raw/ednoda_snapshot_exports/lessons.csv"),
        help="CSV export of lessons table (optional, for grades)",
    )
    parser.add_argument(
        "--lesson-nodes-csv",
        type=Path,
        default=Path("data/raw/ednoda_snapshot_exports/lesson_nodes.csv"),
        help="CSV export of lesson_nodes table (optional, for grades)",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("data/raw/ednoda_snapshot/ednoda_snapshot_cleaned.csv"),
        help="Output snapshot CSV path",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=Path("data/processed/ednoda_snapshot_cleaning_report.json"),
        help="Output cleaning report JSON path",
    )
    parser.add_argument("--include-composites", action="store_true", help="Keep composite_* node types")
    parser.add_argument("--min-text-len", type=int, default=1, help="Minimum node_text length after trimming")
    parser.add_argument("--max-text-len", type=int, default=280, help="Maximum node_text length after trimming")
    parser.add_argument("--max-rows", type=int, default=0, help="Optional cap on output rows (0 = no cap)")
    args = parser.parse_args()

    nodes = _read_csv(args.education_nodes_csv)
    analyses = _read_csv(args.node_analyses_csv) if args.node_analyses_csv.exists() else pd.DataFrame()
    
    textbooks = _read_csv(args.textbooks_csv) if args.textbooks_csv.exists() else pd.DataFrame()
    textbook_nodes = _read_csv(args.textbook_nodes_csv) if args.textbook_nodes_csv.exists() else pd.DataFrame()
    lessons = _read_csv(args.lessons_csv) if args.lessons_csv.exists() else pd.DataFrame()
    lesson_nodes = _read_csv(args.lesson_nodes_csv) if args.lesson_nodes_csv.exists() else pd.DataFrame()

    cols = {c.lower(): c for c in nodes.columns}
    id_col = cols.get("id")
    text_col = cols.get("node_text")
    if not id_col or not text_col:
        raise ValueError("education_nodes export must include id and node_text columns")

    node_type_col = cols.get("node_type")
    deleted_col = cols.get("deleted_at")

    report: dict[str, int] = {"input_rows": int(len(nodes))}

    work = nodes.copy()

    if deleted_col:
        before = len(work)
        work = work[work[deleted_col].isna()].copy()
        report["dropped_deleted_rows"] = int(before - len(work))

    if node_type_col and not args.include_composites:
        before = len(work)
        work = work[work[node_type_col].astype(str).isin(ATOMIC_NODE_TYPES)].copy()
        report["dropped_non_atomic_rows"] = int(before - len(work))

    work["_text_norm"] = work[text_col].map(_normalize_text)
    before = len(work)
    work = work[work["_text_norm"].notna()].copy()
    report["dropped_blank_text_rows"] = int(before - len(work))

    before = len(work)
    work = work[work["_text_norm"].str.len() >= args.min_text_len].copy()
    report["dropped_too_short_rows"] = int(before - len(work))

    before = len(work)
    work = work[work["_text_norm"].str.len() <= args.max_text_len].copy()
    report["dropped_too_long_rows"] = int(before - len(work))

    before = len(work)
    work = work.drop_duplicates(subset=["_text_norm"], keep="first").copy()
    report["dropped_duplicate_text_rows"] = int(before - len(work))

    analysis_latest = _latest_node_analysis(analyses)
    grades_df = _aggregate_node_grades(textbooks, textbook_nodes, lessons, lesson_nodes)

    out = pd.DataFrame(
        {
            "education_node_id": work[id_col].astype("Int64"),
            "text": work["_text_norm"],
            "node_type": work[node_type_col] if node_type_col else pd.NA,
            "difficulty_source": "internal",
            "lang": "en",
            "license_label": "Internal/Private",
            "license_url": pd.NA,
            "is_publicly_redistributable": False,
            "source_created_at": work[cols.get("created_at")] if cols.get("created_at") else pd.NA,
            "source_updated_at": work[cols.get("updated_at")] if cols.get("updated_at") else pd.NA,
        }
    )

    out = out.merge(analysis_latest, on="education_node_id", how="left")
    out = out.merge(grades_df, on="education_node_id", how="left")

    if args.max_rows and args.max_rows > 0:
        out = out.head(args.max_rows).copy()

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output_csv, index=False)

    report["output_rows"] = int(len(out))
    report["analysis_rows_input"] = int(len(analyses))
    report["analysis_rows_joined"] = int(out["analysis_status"].notna().sum()) if "analysis_status" in out.columns else 0
    report["grades_joined"] = int(out["grades"].notna().sum()) if "grades" in out.columns else 0

    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Wrote cleaned snapshot: {args.output_csv}")
    print(f"Wrote cleaning report: {args.report_json}")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
