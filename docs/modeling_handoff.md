# Modeling Handoff

Start with these files:

1. `data/processed/sentences.parquet` (primary sentence corpus)
2. `data/processed/vocabulary_reference.parquet` (lexical reference)
3. `data/processed/grammar_reference.parquet` (grammar reference)
4. `data/processed/validation_summary.json` (quality/provenance checks)
5. `data/processed/build_manifest.json` (build provenance + row counts)
6. `data/processed/day1_summary.md` (quick orientation summary)

Optional next-step table:

7. `data/processed/sentence_features.parquet` (lightweight NLP and readability features after running `scripts/enrich_sentence_features.py`)

For the current day-one setup, these tables are built primarily from:

- `CEFR-SP` for sentence-level recommendations and difficulty-labelled examples
- `CEFR-J` vocabulary for lexical CEFR reference
- `CEFR-J` grammar for grammar-level reference

This means the repo is currently strongest as a clean research starting point for:

- baseline difficulty analysis
- lexical/grammar-aware feature engineering
- retrieval experiments over CEFR-SP sentences
- later addition of richer parsing, embeddings, and ranking models

Recommended first checks:
- Confirm validation has no hard failures.
- Run `python scripts/describe_processed_data.py` if `day1_summary.md` is missing or stale.
- If you need POS/readability features, install the optional NLP stack and run `python scripts/enrich_sentence_features.py`.
- Filter `sentences` by `cefr_level` and `split`.
- Join analyses using `cefr_level`/`cefr_numeric`.
- Verify `source_dataset`, `license_label`, and `license_url` before sharing subsets.

Recommended first modeling questions:

- What is the CEFR distribution of `sentences` by `split` and source subdataset?
- How much useful lexical coverage does `vocabulary_reference` provide for CEFR-SP sentences?
- Which CEFR-J grammar entries are most likely to inform an interpretable grammar-tagging baseline?
