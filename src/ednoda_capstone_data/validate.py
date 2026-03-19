"""Validation checks for processed tables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .schemas import (
    ALLOWED_CEFR_LEVELS,
    GRAMMAR_REFERENCE_COLUMNS,
    LICENSES_COLUMNS,
    SENTENCES_COLUMNS,
    SOURCE_REGISTRY_COLUMNS,
    VOCABULARY_REFERENCE_COLUMNS,
)


def validate_table(df: pd.DataFrame, key_cols: list[str], name: str) -> dict:
    report = {
        "table": name,
        "row_count": int(len(df)),
        "null_counts": df.isna().sum().to_dict(),
        "duplicate_count_on_key": int(df.duplicated(subset=key_cols).sum()) if key_cols else 0,
    }
    if "cefr_level" in df.columns:
        values = df["cefr_level"].dropna().astype(str)
        invalid = sorted(v for v in values.unique() if v not in ALLOWED_CEFR_LEVELS)
        report["cefr_distribution"] = df["cefr_level"].value_counts(dropna=False).to_dict()
        report["invalid_cefr_levels"] = invalid
    if name == "sentences" and not df.empty:
        report["duplicate_record_id"] = int(df.duplicated(subset=["record_id"]).sum())
        report["duplicate_text_cefr"] = int(df.duplicated(subset=["text_normalized", "cefr_level"]).sum())
    return report


def schema_check(df: pd.DataFrame, expected_cols: list[str]) -> dict:
    actual = list(df.columns)
    return {
        "expected": expected_cols,
        "actual": actual,
        "missing": [c for c in expected_cols if c not in actual],
        "extra": [c for c in actual if c not in expected_cols],
        "order_matches": actual == expected_cols,
    }


def read_processed_tables(processed_dir: Path) -> dict[str, pd.DataFrame]:
    names = ["sentences", "vocabulary_reference", "grammar_reference", "source_registry", "licenses"]
    output = {}
    for name in names:
        path = processed_dir / f"{name}.parquet"
        output[name] = pd.read_parquet(path) if path.exists() else pd.DataFrame()
    return output


def expected_schema_map() -> dict[str, list[str]]:
    return {
        "sentences": SENTENCES_COLUMNS,
        "vocabulary_reference": VOCABULARY_REFERENCE_COLUMNS,
        "grammar_reference": GRAMMAR_REFERENCE_COLUMNS,
        "source_registry": SOURCE_REGISTRY_COLUMNS,
        "licenses": LICENSES_COLUMNS,
    }
