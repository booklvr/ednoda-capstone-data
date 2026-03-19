#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from datetime import datetime, timezone

import pandas as pd

from ednoda_capstone_data.io_utils import ensure_columns
from ednoda_capstone_data.schemas import LICENSES_COLUMNS, SENTENCES_COLUMNS, SOURCE_REGISTRY_COLUMNS


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def main() -> None:
    interim = Path("data/interim")
    processed = Path("data/processed")
    processed.mkdir(parents=True, exist_ok=True)

    sentence_frames = [_read(interim / "cefr_sp_sentences.parquet"), _read(interim / "ednoda_sentences.parquet")]
    sentences = pd.concat([f for f in sentence_frames if not f.empty], ignore_index=True) if any(not f.empty for f in sentence_frames) else pd.DataFrame()
    ensure_columns(sentences, SENTENCES_COLUMNS).to_parquet(processed / "sentences.parquet", index=False)

    _read(interim / "cefrj_vocabulary.parquet").to_parquet(processed / "vocabulary_reference.parquet", index=False)
    _read(interim / "cefrj_grammar.parquet").to_parquet(processed / "grammar_reference.parquet", index=False)

    now = _now()
    source_registry = pd.DataFrame(
        [
            {
                "source_dataset": "cefr_sp",
                "source_name": "CEFR-SP",
                "source_url": "https://github.com/yukiar/CEFR-SP",
                "local_raw_path": "data/raw/cefr_sp",
                "download_method": "manual_or_scripted",
                "ingest_script": "scripts/ingest_cefr_sp.py",
                "license_label": "Unknown (verify upstream)",
                "license_url": pd.NA,
                "notes": "Recursive tabular ingestion with heuristic text/cefr detection. Upstream corpus expected under /CEFR-SP in the source repo.",
                "version_or_commit": "unknown",
                "last_verified_utc": now,
                "included_in_v1": True,
            },
            {
                "source_dataset": "cefrj",
                "source_name": "CEFR-J",
                "source_url": "https://github.com/openlanguageprofiles/olp-en-cefrj",
                "local_raw_path": "data/raw/cefrj",
                "download_method": "manual_or_scripted",
                "ingest_script": "scripts/ingest_cefrj_vocabulary.py;scripts/ingest_cefrj_grammar.py",
                "license_label": "CEFR-J terms (verify)",
                "license_url": pd.NA,
                "notes": "Includes vocabulary profile and grammar profile; optional Octanove C1/C2 file. Project site: http://www.cefr-j.org/.",
                "version_or_commit": "1.5 + 20180315",
                "last_verified_utc": now,
                "included_in_v1": True,
            },
            {
                "source_dataset": "ednoda_snapshot",
                "source_name": "Ednoda snapshot",
                "source_url": pd.NA,
                "local_raw_path": "data/raw/ednoda_snapshot",
                "download_method": "internal_export",
                "ingest_script": "scripts/ingest_ednoda_snapshot.py",
                "license_label": "Internal/Private",
                "license_url": pd.NA,
                "notes": "Optional snapshot for research; redistribution likely restricted.",
                "version_or_commit": "snapshot",
                "last_verified_utc": now,
                "included_in_v1": True,
            },
        ]
    )
    ensure_columns(source_registry, SOURCE_REGISTRY_COLUMNS).to_parquet(processed / "source_registry.parquet", index=False)

    licenses = pd.DataFrame(
        [
            {
                "source_dataset": "cefr_sp",
                "license_label": "Unknown (verify upstream)",
                "license_url": pd.NA,
                "usage_notes": "Use for internal research until license verified.",
                "redistribution_notes": "Do not redistribute derived raw text without explicit confirmation.",
                "verified_from": "CEFR-SP source repository",
                "last_verified_utc": now,
            },
            {
                "source_dataset": "cefrj",
                "license_label": "CEFR-J terms (verify)",
                "license_url": "http://www.cefr-j.org/",
                "usage_notes": "Academic research usage expected; confirm terms per dataset files.",
                "redistribution_notes": "Redistribute only when terms clearly permit.",
                "verified_from": "CEFR-J project website",
                "last_verified_utc": now,
            },
            {
                "source_dataset": "ednoda_snapshot",
                "license_label": "Internal/Private",
                "license_url": pd.NA,
                "usage_notes": "Restricted to approved capstone collaborators.",
                "redistribution_notes": "No public redistribution.",
                "verified_from": "Ednoda internal guidance",
                "last_verified_utc": now,
            },
        ]
    )
    ensure_columns(licenses, LICENSES_COLUMNS).to_parquet(processed / "licenses.parquet", index=False)
    print("Merged processed tables.")


if __name__ == "__main__":
    main()
