# Ingestion Guide

## Expected raw source placement

- CEFR-SP root: `data/raw/cefr_sp/`
  - Canonical upstream: https://github.com/yukiar/CEFR-SP
  - Upstream note: corpus is expected under `/CEFR-SP` in the source repo.
  - Local ingest behavior: recursive tabular scan (`.csv/.tsv/.txt/.xlsx/.xls`).
- CEFR-J root: `data/raw/cefrj/`
  - Required exact files:
    - `cefrj-vocabulary-profile-1.5.csv`
    - `cefrj-grammar-profile-20180315.csv`
  - Optional:
    - `octanove-vocabulary-profile-c1c2-1.0.csv`
- Ednoda snapshot (optional): `data/raw/ednoda_snapshot/*.csv`

## Ingestion behavior

- **CEFR-SP** (`scripts/ingest_cefr_sp.py`)
  - Recursively scans tabular files.
  - Logs skipped files and reasons.
  - Records scanned/skipped file information in `metadata_json`.
- **CEFR-J vocabulary** (`scripts/ingest_cefrj_vocabulary.py`)
  - Prefers explicit handling for expected filenames.
  - Falls back to heuristic mapping with clear warning logs when columns differ.
- **CEFR-J grammar** (`scripts/ingest_cefrj_grammar.py`)
  - Prefers explicit handling for expected grammar file.
  - Preserves unmapped fields in `metadata_json`.
- **Ednoda snapshot** (`scripts/ingest_ednoda_snapshot.py`)
  - Ingests first CSV if present.
  - Missing snapshot is treated as optional.
