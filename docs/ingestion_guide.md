# Ingestion Guide

- **CEFR-SP**: recursively scans `data/raw/cefr_sp` for tabular files, heuristically detects text and CEFR columns, normalizes text/CEFR, and writes sentence rows.
- **CEFR-J vocabulary**: reads `cefrj-vocabulary-profile-1.5.csv` and optional `octanove-vocabulary-profile-c1c2-1.0.csv` into `vocabulary_reference`.
- **CEFR-J grammar**: reads `cefrj-grammar-profile-20180315.csv` into `grammar_reference`.
- **Ednoda snapshot**: reads first CSV in `data/raw/ednoda_snapshot` and maps rows into sentence records.
