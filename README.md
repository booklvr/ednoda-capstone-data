# ednoda-capstone-data

Reproducible Python data-prep pipeline for UBC MDS-CL capstone sentence recommendation research.

## Repository structure

```text
ednoda-capstone-data/
├── README.md
├── pyproject.toml
├── requirements.txt
├── .gitignore
├── config/
├── data/
│   ├── raw/
│   │   ├── cefr_sp/
│   │   ├── cefrj/
│   │   └── ednoda_snapshot/
│   ├── interim/
│   └── processed/
├── docs/
│   ├── onboarding.md
│   ├── data_dictionary.md
│   ├── ingestion_guide.md
│   ├── modeling_handoff.md
│   └── licensing_notes.md
├── reports/
├── scripts/
│   ├── download_cefr_sp.py
│   ├── download_cefrj.py
│   ├── ingest_cefr_sp.py
│   ├── ingest_cefrj_vocabulary.py
│   ├── ingest_cefrj_grammar.py
│   ├── ingest_ednoda_snapshot.py
│   ├── merge_processed_tables.py
│   ├── validate_processed_tables.py
│   └── build_all.py
├── src/
│   └── ednoda_capstone_data/
│       ├── __init__.py
│       ├── schemas.py
│       ├── normalize.py
│       ├── io_utils.py
│       ├── ingest.py
│       └── validate.py
└── tests/
    ├── test_normalize.py
    ├── test_schema.py
    └── test_ingest_smoke.py
```

## Processed tables

- `sentences.parquet`
- `vocabulary_reference.parquet`
- `grammar_reference.parquet`
- `source_registry.parquet`
- `licenses.parquet`

Schemas are defined in `src/ednoda_capstone_data/schemas.py`.

## Quickstart

```bash
python -m pip install -e .[dev]
python scripts/build_all.py
pytest -q
```

## Scope

This repo intentionally excludes ML training, embeddings, spaCy, transformers, and API serving.
