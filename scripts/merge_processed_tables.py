#!/usr/bin/env python
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ednoda_capstone_data.io_utils import ensure_columns
from ednoda_capstone_data.schemas import (
    LICENSES_COLUMNS,
    SENTENCES_COLUMNS,
    SOURCE_REGISTRY_COLUMNS,
)


def _read(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def main() -> None:
    interim = Path("data/interim")
    processed = Path("data/processed")
    processed.mkdir(parents=True, exist_ok=True)

    sentence_frames = [
        _read(interim / "cefr_sp_sentences.parquet"),
        _read(interim / "ednoda_sentences.parquet"),
    ]
    sentences = pd.concat([f for f in sentence_frames if not f.empty], ignore_index=True) if any(not f.empty for f in sentence_frames) else pd.DataFrame()
    sentences = ensure_columns(sentences, SENTENCES_COLUMNS)
    sentences.to_parquet(processed / "sentences.parquet", index=False)

    _read(interim / "cefrj_vocabulary.parquet").to_parquet(processed / "vocabulary_reference.parquet", index=False)
    _read(interim / "cefrj_grammar.parquet").to_parquet(processed / "grammar_reference.parquet", index=False)

    registry = pd.DataFrame(
        [
            {
                "source_name": "cefr-sp",
                "source_dataset": "sentence-files",
                "source_version": "unknown",
                "download_url": pd.NA,
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "raw_path": "data/raw/cefr_sp",
                "record_count": int((sentences["source_name"] == "cefr-sp").sum()),
                "checksum_sha256": pd.NA,
                "license_id": "license_cefr_sp",
            },
            {
                "source_name": "cefr-j",
                "source_dataset": "vocabulary+grammar",
                "source_version": "1.5+20180315",
                "download_url": pd.NA,
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "raw_path": "data/raw/cefrj",
                "record_count": int(len(_read(interim / "cefrj_vocabulary.parquet")) + len(_read(interim / "cefrj_grammar.parquet"))),
                "checksum_sha256": pd.NA,
                "license_id": "license_cefrj",
            },
            {
                "source_name": "ednoda",
                "source_dataset": "snapshot",
                "source_version": "optional",
                "download_url": pd.NA,
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "raw_path": "data/raw/ednoda_snapshot",
                "record_count": int((sentences["source_name"] == "ednoda").sum()),
                "checksum_sha256": pd.NA,
                "license_id": "license_ednoda",
            },
        ]
    )
    ensure_columns(registry, SOURCE_REGISTRY_COLUMNS).to_parquet(processed / "source_registry.parquet", index=False)

    licenses = pd.DataFrame(
        [
            {"license_id": "license_cefr_sp", "source_name": "cefr-sp", "license_name": "Unknown", "license_url": pd.NA, "attribution": "See CEFR-SP source", "usage_notes": "Verify before redistribution"},
            {"license_id": "license_cefrj", "source_name": "cefr-j", "license_name": "See CEFR-J terms", "license_url": pd.NA, "attribution": "CEFR-J project", "usage_notes": "Academic/research use"},
            {"license_id": "license_ednoda", "source_name": "ednoda", "license_name": "Private/Internal", "license_url": pd.NA, "attribution": "Ednoda", "usage_notes": "Optional snapshot"},
        ]
    )
    ensure_columns(licenses, LICENSES_COLUMNS).to_parquet(processed / "licenses.parquet", index=False)
    print("Merged processed tables.")


if __name__ == "__main__":
    main()
