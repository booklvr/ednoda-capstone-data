#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import json

from ednoda_capstone_data.sources import RAW_CEFRJ_DIR, RAW_CEFR_SP_DIR, discover_cefr_sp_tabular_files, required_cefrj_paths
from ednoda_capstone_data.validate import expected_schema_map, read_processed_tables, schema_check, validate_table


def _markdown_section(title: str, body: str) -> str:
    return f"## {title}\n\n{body}\n"


def main() -> None:
    processed = Path("data/processed")
    processed.mkdir(parents=True, exist_ok=True)
    tables = read_processed_tables(processed)

    key_map = {
        "sentences": ["record_id"],
        "vocabulary_reference": ["record_id"],
        "grammar_reference": ["record_id"],
        "source_registry": ["source_dataset"],
        "licenses": ["source_dataset", "license_label"],
    }
    expected = expected_schema_map()
    summary = {name: validate_table(df, key_map[name], name) for name, df in tables.items()}
    summary["schema_checks"] = {name: schema_check(df, expected[name]) for name, df in tables.items()}

    failures: list[str] = []
    for name in ["sentences", "vocabulary_reference", "grammar_reference"]:
        sch = summary["schema_checks"][name]
        if sch["missing"]:
            failures.append(f"{name}: missing required columns {sch['missing']}")

    cefr_sp_present = len(discover_cefr_sp_tabular_files(RAW_CEFR_SP_DIR)) > 0
    cefrj_paths = required_cefrj_paths(RAW_CEFRJ_DIR)
    cefrj_vocab_present = cefrj_paths["vocabulary"].exists()
    cefrj_grammar_present = cefrj_paths["grammar"].exists()

    if cefr_sp_present and summary["sentences"]["row_count"] == 0:
        failures.append("CEFR-SP raw files are present but sentences produced zero rows")
    if cefrj_vocab_present and summary["vocabulary_reference"]["row_count"] == 0:
        failures.append("CEFR-J vocabulary file is present but vocabulary_reference produced zero rows")
    if cefrj_grammar_present and summary["grammar_reference"]["row_count"] == 0:
        failures.append("CEFR-J grammar file is present but grammar_reference produced zero rows")

    for table_name in ["sentences", "vocabulary_reference", "grammar_reference"]:
        invalid = summary[table_name].get("invalid_cefr_levels", [])
        if invalid:
            failures.append(f"{table_name}: invalid CEFR values found: {invalid}")

    sentences_df = tables["sentences"]
    if not sentences_df.empty and "text_normalized" in sentences_df.columns:
        empty_text = int((sentences_df["text_normalized"].fillna("").astype(str).str.strip() == "").sum())
        if empty_text > 0:
            failures.append(f"sentences: {empty_text} rows have empty text_normalized")

    summary["hard_failures"] = failures
    (processed / "validation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    row_md = "\n".join([f"- **{name}**: {rep['row_count']}" for name, rep in summary.items() if isinstance(rep, dict) and "row_count" in rep])
    (processed / "validation_row_counts.md").write_text(_markdown_section("Row Counts", row_md), encoding="utf-8")

    null_lines = []
    dup_lines = []
    schema_lines = []
    for name in tables:
        rep = summary[name]
        null_lines.append(f"### {name}\n- Null counts: `{json.dumps(rep['null_counts'])}`")
        dup_lines.append(f"### {name}\n- Duplicate on key: {rep['duplicate_count_on_key']}")
        if name == "sentences":
            dup_lines.append(f"- Duplicate `record_id`: {rep.get('duplicate_record_id', 0)}")
            dup_lines.append(f"- Duplicate `text_normalized + cefr_level`: {rep.get('duplicate_text_cefr', 0)}")
        if "invalid_cefr_levels" in rep:
            dup_lines.append(f"- Invalid CEFR levels: {rep['invalid_cefr_levels']}")

        sch = summary["schema_checks"][name]
        schema_lines.append(f"### {name}\n- Order matches: {sch['order_matches']}\n- Missing: {sch['missing']}\n- Extra: {sch['extra']}")

    (processed / "validation_null_checks.md").write_text(_markdown_section("Null Checks", "\n".join(null_lines)), encoding="utf-8")
    (processed / "validation_duplicate_checks.md").write_text(_markdown_section("Duplicate Checks", "\n".join(dup_lines)), encoding="utf-8")
    (processed / "validation_schema_checks.md").write_text(_markdown_section("Schema Checks", "\n".join(schema_lines)), encoding="utf-8")

    if failures:
        print("[FAIL] Validation hard failures detected:")
        for failure in failures:
            print(f"- {failure}")
        raise SystemExit(1)

    print(f"Wrote validation outputs under {processed}")


if __name__ == "__main__":
    main()
