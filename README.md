# ednoda-capstone-data

Reproducible Python data-prep pipeline for UBC MDS-CL capstone sentence recommendation research.

## Source provenance (v1)

- **CEFR-SP**
  - Canonical upstream repo: https://github.com/yukiar/CEFR-SP
  - Upstream corpus is expected under `/CEFR-SP` in that repository.
  - Local placement for this repo: `data/raw/cefr_sp/` (recursive scan for tabular files).
- **CEFR-J**
  - GitHub repo: https://github.com/openlanguageprofiles/olp-en-cefrj
  - Project site: http://www.cefr-j.org/
  - Local placement: `data/raw/cefrj/`
  - Required files:
    - `cefrj-vocabulary-profile-1.5.csv`
    - `cefrj-grammar-profile-20180315.csv`
  - Optional file:
    - `octanove-vocabulary-profile-c1c2-1.0.csv`
- **Ednoda snapshot (optional)**
  - Local placement: `data/raw/ednoda_snapshot/` (first CSV will be ingested).

## Quickstart

### Option A: requirements-only install (offline-friendly)

```bash
python -m pip install -r requirements.txt
PYTHONPATH=src pytest -q
python scripts/preflight_check.py
python scripts/build_all.py
```

### Option B: editable install (if supported)

```bash
python -m pip install --no-build-isolation -e .[dev]
pytest -q
python scripts/preflight_check.py
python scripts/build_all.py
```

If editable install fails in constrained environments, use Option A.

## Data layers

- `data/raw/`: source files as downloaded/exported, no destructive edits.
- `data/interim/`: source-specific ingested parquet outputs.
- `data/processed/`: standardized capstone v1 tables, validation summaries, and build manifest.

## Operations

- Preflight check: `python scripts/preflight_check.py`
- Tests: `PYTHONPATH=src pytest -q`
- Full build: `python scripts/build_all.py`

## What success looks like

- `scripts/preflight_check.py` shows required CEFR-SP + CEFR-J files present.
- `scripts/build_all.py` completes and writes:
  - processed parquet tables
  - `data/processed/validation_summary.json`
  - `data/processed/build_manifest.json`
- Validation exits cleanly with no hard failures.

## Common failures

- **Missing CEFR-J files**: ensure exact filenames under `data/raw/cefrj/`.
- **No CEFR-SP rows**: verify tabular data exists under `data/raw/cefr_sp/` and includes sentence-like columns.
- **Invalid CEFR levels**: source values outside A1..C2 are rejected/flagged.

## Scope

This repo intentionally excludes ML training, embeddings, spaCy, transformers, torch, and API serving.

## Future dataset candidates

- v1 currently focuses on CEFR-SP, CEFR-J, and the optional Ednoda snapshot.
- Future candidate datasets are tracked in `docs/future_dataset_candidates.md`.
- These candidates are not yet integrated into the pipeline.

