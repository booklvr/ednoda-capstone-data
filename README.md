# ednoda-capstone-data

Reproducible Python data-prep pipeline for the Ednoda x UBC MDS-CL capstone on curriculum-aligned ESL sentence recommendations.

This repository is the data foundation for the capstone project. Its job is to ingest, normalize, validate, and document the datasets that the student team will later use for grammar profiling, difficulty modeling, and sentence recommendation experiments.

## What This Repo Is

- A clean Python package plus scripts for turning raw source files into standardized parquet tables
- A reproducible starting point for UBC students joining the project
- A lightweight, CPU-friendly pipeline that runs without ML training or GPU setup

## What This Repo Is Not

- Not the full recommendation engine yet
- Not a web app or API
- Not the final modeling notebook deliverable
- Not a bundled copy of the public datasets; students still need to place raw files in `data/raw/`

## Current Day-1 Scope

For the initial capstone handoff, the recommended working set is:

- `CEFR-SP` for sentence-level difficulty-labelled examples
- `CEFR-J` vocabulary for lexical CEFR reference
- `CEFR-J` grammar for grammar-profile reference

`UD English-ESL`, `Wiki-Auto`, and `UniversalCEFR` are intentionally deferred from the default day-one setup. They may still be useful later, but they are not required to get the repo into a healthy research-ready state.

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

### Recommended setup

Use `python3` explicitly. On many machines, including this one, `python` is not available on `PATH` even when Python is installed.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest -q
python scripts/fetch_public_data.py cefr-sp cefr-j
python scripts/preflight_check.py
python scripts/build_all.py
```

At this point:

- passing tests means the codebase itself is healthy
- the fetch step pulls the current recommended day-one datasets
- the build writes the processed tables students will start from

### Editable install option

```bash
python -m pip install --no-build-isolation -e .[dev]
python -m pytest -q
python scripts/preflight_check.py
```

If editable install fails in a constrained environment, use the standard `requirements.txt` flow above.

### Full build after raw data is present

```bash
python scripts/build_all.py
```

Successful build outputs:

- `data/processed/sentences.parquet`
- `data/processed/vocabulary_reference.parquet`
- `data/processed/grammar_reference.parquet`
- `data/processed/source_registry.parquet`
- `data/processed/licenses.parquet`
- `data/processed/validation_summary.json`
- `data/processed/build_manifest.json`

## Data layers

- `data/raw/`: source files as downloaded/exported, no destructive edits.
- `data/interim/`: source-specific ingested parquet outputs.
- `data/processed/`: standardized capstone v1 tables, validation summaries, and build manifest.

## Raw Data Expected

- CEFR-SP: place tabular source files under `data/raw/cefr_sp/`
- CEFR-J: place required files under `data/raw/cefrj/`
- Ednoda snapshot: optional CSV under `data/raw/ednoda_snapshot/`

This repository does not currently ship with those datasets included.

## Operations

- Preflight check: `python scripts/preflight_check.py`
- Tests: `python -m pytest -q`
- Fetch recommended day-one datasets: `python scripts/fetch_public_data.py cefr-sp cefr-j`
- List available dataset keys: `python scripts/fetch_public_data.py --list`
- Full build: `python scripts/build_all.py`
- Day-1 data summary: `python scripts/describe_processed_data.py`

## First-Time Contributor Workflow

1. Create and activate `.venv`.
2. Install dependencies.
3. Run `python -m pytest -q`.
4. Run `python scripts/fetch_public_data.py cefr-sp cefr-j`.
5. Run `python scripts/preflight_check.py`.
6. Run `python scripts/build_all.py`.
7. Run `python scripts/describe_processed_data.py`.
8. Inspect `data/processed/day1_summary.md`, `data/processed/validation_summary.json`, and `data/processed/build_manifest.json`.

## What success looks like

- `python -m pytest -q` passes.
- `scripts/preflight_check.py` shows required CEFR-SP + CEFR-J files present once data is added.
- `scripts/build_all.py` completes and writes:
  - processed parquet tables
  - `data/processed/validation_summary.json`
  - `data/processed/build_manifest.json`
- Validation exits cleanly with no hard failures.

## Re-running The Build

`python scripts/build_all.py` is intended to be safe to rerun.

- The pipeline rewrites the fixed parquet outputs in `data/interim/` and `data/processed/`
- It does not append new rows onto old parquet files
- Running the build multiple times should not create duplicate growth by itself

Note:

- timestamps like `created_at_utc` may change across rebuilds
- if the raw source files change, the rebuilt outputs may also change

## Common failures

- **`python: command not found`**: use `python3` to create the virtual environment, then activate `.venv` and use `python`.
- **`pytest: command not found`**: run `python -m pytest -q` instead of relying on a shell-level `pytest` executable.
- **Missing CEFR-J files**: ensure exact filenames under `data/raw/cefrj/`.
- **No CEFR-SP rows**: verify tabular data exists under `data/raw/cefr_sp/` and includes sentence-like columns.
- **Invalid CEFR levels**: source values outside A1..C2 are rejected/flagged.
- **Preflight shows missing data**: expected until raw datasets have been downloaded or exported into `data/raw/`.

## Scope

This repo intentionally excludes ML training, embeddings, spaCy, transformers, torch, and API serving.

Lightweight feature extraction is now supported as an optional next step, but it is still separate from full modeling/training.

## Optional NLP Enrichment

If you want day-one NLP features on the processed sentences table, install the optional NLP stack and run:

```bash
python -m pip install '.[nlp]'
python -m spacy download en_core_web_sm
python scripts/enrich_sentence_features.py
```

This writes `data/processed/sentence_features.parquet` with:

- POS-tag-derived counts
- lexical density
- token and length features
- textstat readability metrics

This is intended as a lightweight baseline feature table for students, not a finished difficulty model.


## Future dataset candidates

- v1 currently focuses on CEFR-SP, CEFR-J, and the optional Ednoda snapshot.
- Future candidate datasets are tracked in `docs/future_dataset_candidates.md`.
- These candidates are not yet integrated into the pipeline.

## Acquisition Guide

- See `docs/dataset_acquisition.md` for current source links, licensing notes, and the scripted/manual acquisition split.
