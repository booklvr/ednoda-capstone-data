"""Common IO helpers."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

TABULAR_SUFFIXES = {".csv", ".tsv", ".txt", ".xlsx", ".xls"}


def read_tabular(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".tsv", ".txt"}:
        return pd.read_csv(path, sep="\t")
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported tabular file: {path}")


def ensure_columns(df: pd.DataFrame, ordered_columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in ordered_columns:
        if col not in out.columns:
            out[col] = pd.NA
    return out[ordered_columns]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
