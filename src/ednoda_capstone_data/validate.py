"""Validation checks for processed tables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .schemas import ALLOWED_CEFR_LEVELS


def validate_table(df: pd.DataFrame, key_cols: list[str], name: str) -> dict:
    report = {
        "table": name,
        "row_count": int(len(df)),
        "null_counts": df.isna().sum().to_dict(),
        "duplicate_count": int(df.duplicated(subset=key_cols).sum()) if key_cols else 0,
    }
    if "cefr_level" in df.columns:
        report["cefr_distribution"] = df["cefr_level"].value_counts(dropna=False).to_dict()
        invalid = sorted(set(df["cefr_level"].dropna()) - set(ALLOWED_CEFR_LEVELS))
        report["invalid_cefr_levels"] = invalid
    if "source_name" in df.columns:
        report["source_distribution"] = df["source_name"].value_counts(dropna=False).to_dict()
    return report


def read_processed_tables(processed_dir: Path) -> dict[str, pd.DataFrame]:
    names = [
        "sentences",
        "vocabulary_reference",
        "grammar_reference",
        "source_registry",
        "licenses",
    ]
    output = {}
    for name in names:
        path = processed_dir / f"{name}.parquet"
        output[name] = pd.read_parquet(path) if path.exists() else pd.DataFrame()
    return output
