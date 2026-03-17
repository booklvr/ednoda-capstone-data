#!/usr/bin/env python
from __future__ import annotations

import json
from pathlib import Path

from ednoda_capstone_data.validate import read_processed_tables, validate_table


def main() -> None:
    processed = Path("data/processed")
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    tables = read_processed_tables(processed)
    key_map = {
        "sentences": ["sentence_id"],
        "vocabulary_reference": ["vocab_id"],
        "grammar_reference": ["grammar_id"],
        "source_registry": ["source_name", "source_dataset"],
        "licenses": ["license_id"],
    }
    report = {name: validate_table(df, key_map[name], name) for name, df in tables.items()}
    out = reports_dir / "validation_report.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
