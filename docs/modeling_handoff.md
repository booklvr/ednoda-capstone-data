# Modeling Handoff

Start with these files:

1. `data/processed/sentences.parquet` (primary sentence corpus)
2. `data/processed/vocabulary_reference.parquet` (lexical reference)
3. `data/processed/grammar_reference.parquet` (grammar reference)
4. `data/processed/validation_summary.json` (quality/provenance checks)
5. `data/processed/build_manifest.json` (build provenance + row counts)

Recommended first checks:
- Confirm validation has no hard failures.
- Filter `sentences` by `cefr_level` and `split`.
- Join analyses using `cefr_level`/`cefr_numeric`.
- Verify `source_dataset`, `license_label`, and `license_url` before sharing subsets.
