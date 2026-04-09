# Production Snapshot Provenance (Ednoda)

> [!NOTE]
> This document is intended as a record of provenance for the UBC MDS-CL student team. It outlines how the internal Ednoda dataset (`ednoda_snapshot`) was securely exported and transformed prior to handoff. Students do not need to perform these steps, as the internal data has already been ingested.

## Architecture & Security

To maintain strict isolation from production systems, the Ednoda snapshot was generated using an offline, two-step export and transform workflow:

1. Securely export target tables (`education_nodes`, `node_analyses`, textbooks, and lessons) from the production database via pgAdmin.
2. Run a local offline preparation script to clean, filter, and join the exported CSVs into a single canonical snapshot.

This approach ensures the capstone research environment remains independent and secure while providing high-fidelity, real-world data for NLP experiments.

---

## Extracted Data Models

### `education_nodes` (high value; primary table)

Key fields maintained for capstone sentence recommendations:

- `id` -> stable identifier (`education_node_id`)
- `node_text` -> sentence/content text (core modeling input)
- `node_type` -> useful filtering/stratification (`vocab`, `expression`, `question`, `phonics`)
- `deleted_at` -> data quality/safety filter
- `created_at`, `updated_at` -> lineage/debug metadata

### `node_analyses` (optional enrichment)

Useful as lightweight NLP metadata:

- `education_node_id` -> join key to `education_nodes.id`
- `status` -> keep `completed`
- `language_code` -> language QA
- `analysis_version` -> reproducibility/versioning
- `token_count`, `sentence_count` -> helpful features/quality checks
- `analysis` -> the full spaCy NLP JSON result (tokens, POS tags, etc.)
- `processed_at` -> pick latest completed analysis per node

The full JSON `analysis` blob contains the spaCy tokens, POS tags, and dependency parses. It is the primary input for grammar profiling and difficulty modeling.

---

## Transformation Pipeline Rules

The preparation script (`scripts/prepare_ednoda_snapshot_from_exports.py`) enforces the following rules before ingestion:

1. **Soft-delete filter**: keep `deleted_at IS NULL`.
2. **Atomic node filter**: keep `node_type IN ('vocab','expression','question','phonics')`.
3. **Text quality filter**:
   - remove null/blank `node_text`
   - trim whitespace
   - drop extremely short/long rows (default 5..280 chars)
5. **Deduplicate** by normalized text.
6. **Analysis join (optional)**: left join latest completed `node_analyses` row per node.
7. **Privacy**: do not export PII columns from related tables.

---

## Export Queries (Internal SOP)

### A) Export `education_nodes`

1. In pgAdmin, open your production DB.
2. Right click `education_nodes` table -> **Import/Export Data...**
3. Choose **Export**.
4. Format: **CSV**.
5. File name: `education_nodes.csv`.
6. Enable **Header**.
7. Prefer UTF-8 encoding.
8. Use a query (recommended) to export only needed columns/rows:

```sql
SELECT
  id,
  node_text,
  node_type,
  created_at,
  updated_at,
  deleted_at
FROM education_nodes
```

### B) Export `node_analyses` (optional)

Same flow, file `node_analyses.csv`, query:

```sql
SELECT
  education_node_id,
  status,
  language_code,
  analysis_version,
  token_count,
  sentence_count,
  source_text_hash,
  requested_at,
  processed_at,
  analysis
FROM node_analyses
```

### C) Export textbook mapping (optional, for grades)

Same flow, files `textbooks.csv` and `textbook_nodes.csv`.

```sql
SELECT id, grade, deleted_at FROM textbooks
```
```sql
SELECT education_node_id, textbook_id, deleted_at FROM textbook_nodes
```

### D) Export lesson mapping (optional, for grades)

Same flow, files `lessons.csv` and `lesson_nodes.csv`.

```sql
SELECT id, grade, deleted_at FROM lessons
```
```sql
SELECT education_node_id, lesson_id, deleted_at FROM lesson_nodes
```

---

## Pre-Handoff Generation

The exported CSVs were placed into the repository locally:

- `data/raw/ednoda_snapshot_exports/education_nodes.csv`
- `data/raw/ednoda_snapshot_exports/node_analyses.csv` (optional)
- `data/raw/ednoda_snapshot_exports/textbooks.csv` (optional)
- `data/raw/ednoda_snapshot_exports/textbook_nodes.csv` (optional)
- `data/raw/ednoda_snapshot_exports/lessons.csv` (optional)
- `data/raw/ednoda_snapshot_exports/lesson_nodes.csv` (optional)

---

## Build cleaned snapshot + ingest

Run:

```bash
python scripts/prepare_ednoda_snapshot_from_exports.py
python scripts/ingest_ednoda_snapshot.py
python scripts/build_all.py
python scripts/describe_processed_data.py
```

The prep script writes:

- cleaned snapshot CSV: `data/raw/ednoda_snapshot/ednoda_snapshot_cleaned.csv`
- cleaning report JSON: `data/processed/ednoda_snapshot_cleaning_report.json`

### Common options

```bash
# keep composites too
python scripts/prepare_ednoda_snapshot_from_exports.py --include-composites

# cap to a smaller subset for first pass
python scripts/prepare_ednoda_snapshot_from_exports.py --max-rows 5000
```

---

## Validation checklist

After running the commands:

- `data/raw/ednoda_snapshot/ednoda_snapshot_cleaned.csv` exists.
- `data/interim/ednoda_sentences.parquet` exists.
- `data/processed/sentences.parquet` includes `source_dataset = 'ednoda_snapshot'` rows.
- `data/processed/ednoda_snapshot_cleaning_report.json` shows rows dropped by each cleaning rule.
- `data/processed/build_manifest.json` records discovered snapshot files.

---

## Notes on "uploading" into the capstone project

For this repo, "upload" simply means placing CSV files into the expected `data/raw/...` folders locally, then running scripts. No separate cloud upload step is required unless your team has its own storage workflow.
