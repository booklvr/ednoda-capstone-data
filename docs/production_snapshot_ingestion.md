# Production Snapshot Ingestion (Ednoda)

This guide is for your exact use case:

- you want to use production-derived data in the capstone pipeline
- you **do not** want this repo to connect to production directly
- you want to clean/select a subset before ingestion

## Recommended architecture

Use a **two-export + local transform** workflow:

1. Export `education_nodes` from pgAdmin to CSV.
2. Export `node_analyses` from pgAdmin to CSV (optional but useful).
3. Run a local prep script in this repo to clean/filter/join them.
4. Ingest the cleaned snapshot via existing pipeline scripts.

This keeps production isolated while still letting you include high-value internal data.

---

## What data is useful from your two models

### `education_nodes` (high value; primary table)

Most useful fields for capstone sentence recommendations:

- `id` -> stable identifier (`education_node_id`)
- `node_text` -> sentence/content text (core modeling input)
- `node_type` -> useful filtering/stratification (`vocab`, `expression`, `question`, `phonics`)
- `visibility`, `moderation_status`, `deleted_at` -> data quality/safety filters
- `created_at`, `updated_at` -> lineage/debug metadata

### `node_analyses` (optional enrichment)

Useful as lightweight NLP metadata:

- `education_node_id` -> join key to `education_nodes.id`
- `status` -> keep `completed`
- `language_code` -> language QA
- `analysis_version` -> reproducibility/versioning
- `token_count`, `sentence_count` -> helpful features/quality checks
- `processed_at` -> pick latest completed analysis per node

The full JSON `analysis` blob is usually too heavy for day-1 capstone data prep; keep it out unless you have a concrete downstream need.

---

## Cleaning strategy (recommended defaults)

Apply these rules before ingestion:

1. **Soft-delete filter**: keep `deleted_at IS NULL`.
2. **Publishing filter**: keep `visibility='public'` and `moderation_status='approved'` (unless you deliberately want drafts).
3. **Atomic node filter**: keep `node_type IN ('vocab','expression','question','phonics')`.
4. **Text quality filter**:
   - remove null/blank `node_text`
   - trim whitespace
   - drop extremely short/long rows (default 5..280 chars)
5. **Deduplicate** by normalized text.
6. **Analysis join (optional)**: left join latest completed `node_analyses` row per node.
7. **Privacy**: do not export PII columns from related tables.

---

## pgAdmin export (step-by-step)

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
  visibility,
  moderation_status,
  created_at,
  updated_at,
  deleted_at
FROM education_nodes;
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
  processed_at
FROM node_analyses;
```

---

## Put exported files into this repo

Copy CSVs here:

- `data/raw/ednoda_snapshot_exports/education_nodes.csv`
- `data/raw/ednoda_snapshot_exports/node_analyses.csv` (optional)

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
# include private/unapproved rows if you need them for experiments
python scripts/prepare_ednoda_snapshot_from_exports.py --include-private --include-unapproved

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
