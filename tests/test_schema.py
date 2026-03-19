from ednoda_capstone_data.schemas import LICENSES_COLUMNS, SENTENCES_COLUMNS, SOURCE_REGISTRY_COLUMNS


def test_required_sentence_columns_present():
    required = {"record_id", "source_dataset", "text", "text_normalized", "cefr_level", "cefr_numeric", "created_at_utc"}
    assert required.issubset(set(SENTENCES_COLUMNS))


def test_source_registry_shape():
    assert SOURCE_REGISTRY_COLUMNS == [
        "source_dataset",
        "source_name",
        "source_url",
        "local_raw_path",
        "download_method",
        "ingest_script",
        "license_label",
        "license_url",
        "notes",
        "version_or_commit",
        "last_verified_utc",
        "included_in_v1",
    ]


def test_license_table_shape():
    assert LICENSES_COLUMNS == [
        "source_dataset",
        "license_label",
        "license_url",
        "usage_notes",
        "redistribution_notes",
        "verified_from",
        "last_verified_utc",
    ]
