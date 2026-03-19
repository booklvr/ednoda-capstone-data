# Ingestion Guide

## Expected raw source placement

- CEFR-SP root: `data/raw/cefr_sp/`
  - Canonical upstream: https://github.com/yukiar/CEFR-SP
  - Upstream note: corpus is expected under `/CEFR-SP` in the source repo.
  - Recommended acquisition: `python scripts/fetch_public_data.py cefr-sp`
  - Local ingest behavior: recursive tabular scan (`.csv/.tsv/.txt/.xlsx/.xls`).
  - Current upstream format used in v1: headerless tab-delimited text files in `SCoRE/` and `Wiki-Auto/`.
- CEFR-J root: `data/raw/cefrj/`
  - Recommended acquisition: `python scripts/fetch_public_data.py cefr-j`
  - Required exact files:
    - `cefrj-vocabulary-profile-1.5.csv`
    - `cefrj-grammar-profile-20180315.csv`
  - Optional:
    - `octanove-vocabulary-profile-c1c2-1.0.csv`
- Ednoda snapshot (optional): `data/raw/ednoda_snapshot/*.csv`

## Ingestion behavior

- **CEFR-SP** (`scripts/ingest_cefr_sp.py`)
  - Recursively scans tabular files.
  - Recognizes the headerless CEFR-SP tab-delimited format and resolves the two annotator labels into a single canonical CEFR band.
  - Records scanned/skipped file information in `metadata_json`.
- **CEFR-J vocabulary** (`scripts/ingest_cefrj_vocabulary.py`)
  - Uses explicit column mappings for the current CEFR-J vocabulary files.
  - Preserves extra source metadata such as core inventory fields and notes in `metadata_json`.
- **CEFR-J grammar** (`scripts/ingest_cefrj_grammar.py`)
  - Uses explicit column mappings for the current CEFR-J grammar profile.
  - Normalizes CEFR-J levels such as `A1.1` into canonical CEFR bands like `A1`.
  - Preserves extra source metadata such as shorthand code, EGP, GSELO, and notes in `metadata_json`.
- **Ednoda snapshot** (`scripts/ingest_ednoda_snapshot.py`)
  - Ingests first CSV if present.
  - Missing snapshot is treated as optional.
  - Preserves exported textbook/unit metadata inside `metadata_json`.
  - Supports exported identifiers such as `education_node_id`, plus textbook/unit/region/grade columns when present.
