"""Canonical processed-table schemas."""

SENTENCES_COLUMNS = [
    "sentence_id",
    "source_name",
    "source_dataset",
    "source_record_id",
    "text",
    "text_normalized",
    "language",
    "cefr_level",
    "topic",
    "license_id",
    "metadata_json",
]

VOCABULARY_REFERENCE_COLUMNS = [
    "vocab_id",
    "source_name",
    "source_dataset",
    "headword",
    "lemma",
    "pos",
    "cefr_level",
    "frequency_band",
    "example",
    "notes",
    "license_id",
]

GRAMMAR_REFERENCE_COLUMNS = [
    "grammar_id",
    "source_name",
    "source_dataset",
    "grammar_label",
    "grammar_subcategory",
    "pattern",
    "description",
    "cefr_level",
    "example",
    "notes",
    "license_id",
]

SOURCE_REGISTRY_COLUMNS = [
    "source_name",
    "source_dataset",
    "source_version",
    "download_url",
    "downloaded_at_utc",
    "raw_path",
    "record_count",
    "checksum_sha256",
    "license_id",
]

LICENSES_COLUMNS = [
    "license_id",
    "source_name",
    "license_name",
    "license_url",
    "attribution",
    "usage_notes",
]

ALLOWED_CEFR_LEVELS = ["A1", "A2", "B1", "B2", "C1", "C2"]
