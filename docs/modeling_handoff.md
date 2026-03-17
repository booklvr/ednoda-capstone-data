# Modeling Handoff

Use `data/processed/sentences.parquet` as the core recommendation corpus and join to references by CEFR and text metadata.

Recommended checks before modeling:
- CEFR balance (`validation_report.json`)
- Duplicates by `text_normalized`
- Source mix across CEFR bands
