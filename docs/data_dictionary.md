# Data Dictionary (v1)

## sentences
`record_id, source_dataset, source_subdataset, source_record_id, text, text_normalized, lang, granularity, cefr_level, cefr_numeric, difficulty_source, topic_hint, grammar_hint, grade_hint, region_hint, node_type, license_label, license_url, is_publicly_redistributable, split, metadata_json, created_at_utc`

## vocabulary_reference
`record_id, source_dataset, source_record_id, headword, lemma, surface_form, pos, cefr_level, cefr_numeric, list_name, license_label, license_url, metadata_json, created_at_utc`

## grammar_reference
`record_id, source_dataset, source_record_id, grammar_item, grammar_description, cefr_level, cefr_numeric, framework, tags_json, examples_json, license_label, license_url, metadata_json, created_at_utc`

## source_registry
`source_dataset, source_name, source_url, local_raw_path, download_method, ingest_script, license_label, license_url, notes, version_or_commit, last_verified_utc, included_in_v1`

## licenses
`source_dataset, license_label, license_url, usage_notes, redistribution_notes, verified_from, last_verified_utc`
