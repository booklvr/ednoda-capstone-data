#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import json

import pandas as pd


def _format_counts(series: pd.Series, limit: int = 10) -> list[str]:
    if series.empty:
        return ["- none"]
    lines: list[str] = []
    for key, value in series.head(limit).items():
        label = "NULL" if pd.isna(key) else str(key)
        lines.append(f"- `{label}`: {int(value)}")
    return lines


def _section(title: str, lines: list[str]) -> str:
    body = "\n".join(lines) if lines else "- none"
    return f"## {title}\n\n{body}\n"


def main() -> None:
    processed = Path("data/processed")
    summary_path = processed / "day1_summary.md"

    sentences = pd.read_parquet(processed / "sentences.parquet")
    vocabulary = pd.read_parquet(processed / "vocabulary_reference.parquet")
    grammar = pd.read_parquet(processed / "grammar_reference.parquet")

    validation_summary = json.loads((processed / "validation_summary.json").read_text(encoding="utf-8"))
    build_manifest = json.loads((processed / "build_manifest.json").read_text(encoding="utf-8"))

    report_parts: list[str] = ["# Day-1 Data Summary\n"]

    report_parts.append(
        _section(
            "Build Snapshot",
            [
                f"- `sentences`: {len(sentences)} rows",
                f"- `vocabulary_reference`: {len(vocabulary)} rows",
                f"- `grammar_reference`: {len(grammar)} rows",
                f"- hard failures: {len(validation_summary.get('hard_failures', []))}",
                f"- build finished: `{build_manifest.get('build_finished_at_utc', 'unknown')}`",
            ],
        )
    )

    report_parts.append(
        _section(
            "Sentences Overview",
            [
                "- CEFR distribution:",
                * _format_counts(sentences["cefr_level"].value_counts(dropna=False)),
                "- Split distribution:",
                * _format_counts(sentences["split"].value_counts(dropna=False)),
                "- Source subdataset distribution:",
                * _format_counts(sentences["source_subdataset"].value_counts(dropna=False), limit=12),
            ],
        )
    )

    report_parts.append(
        _section(
            "Vocabulary Overview",
            [
                "- CEFR distribution:",
                * _format_counts(vocabulary["cefr_level"].value_counts(dropna=False)),
                "- POS distribution:",
                * _format_counts(vocabulary["pos"].value_counts(dropna=False), limit=12),
                "- List distribution:",
                * _format_counts(vocabulary["list_name"].value_counts(dropna=False)),
            ],
        )
    )

    report_parts.append(
        _section(
            "Grammar Overview",
            [
                "- CEFR distribution:",
                * _format_counts(grammar["cefr_level"].value_counts(dropna=False)),
                "- Example grammar items:",
                * [f"- `{item}`" for item in grammar["grammar_item"].dropna().astype(str).head(10).tolist()],
            ],
        )
    )

    report_parts.append(
        _section(
            "Suggested First Questions",
            [
                "- Which CEFR levels are over- or under-represented in `sentences`?",
                "- Are the `Wiki-Auto` and `SCoRE` sentence pools different enough to matter for retrieval or evaluation?",
                "- How much CEFR-J vocabulary coverage do common CEFR-SP sentences receive?",
                "- Which CEFR-J grammar entries are easiest to convert into an interpretable baseline tagger?",
            ],
        )
    )

    summary_path.write_text("\n".join(report_parts), encoding="utf-8")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
