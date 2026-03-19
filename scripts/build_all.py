#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import json
import subprocess
from datetime import datetime, timezone

import pandas as pd

from ednoda_capstone_data.sources import (
    RAW_CEFRJ_DIR,
    RAW_CEFR_SP_DIR,
    RAW_EDNODA_DIR,
    discover_cefr_sp_tabular_files,
    discover_ednoda_snapshot_files,
    optional_cefrj_paths,
    required_cefrj_paths,
)

SCRIPTS = [
    "scripts/preflight_check.py --for-build",
    "scripts/ingest_cefr_sp.py",
    "scripts/ingest_cefrj_vocabulary.py",
    "scripts/ingest_cefrj_grammar.py",
    "scripts/ingest_ednoda_snapshot.py",
    "scripts/merge_processed_tables.py",
    "scripts/validate_processed_tables.py",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_count(path: Path) -> int:
    if not path.exists():
        return 0
    return int(len(pd.read_parquet(path)))


def main() -> None:
    started = _now()
    warnings: list[str] = []
    scripts_run: list[str] = []

    print("[BUILD] Starting build pipeline")
    for i, command in enumerate(SCRIPTS, start=1):
        print(f"[BUILD] Step {i}/{len(SCRIPTS)}: {command}")
        parts = command.split(" ")
        subprocess.run([sys.executable, *parts], check=True)
        scripts_run.append(command)

    processed = Path("data/processed")
    manifest = {
        "build_started_at_utc": started,
        "build_finished_at_utc": _now(),
        "scripts_run": scripts_run,
        "raw_source_paths_discovered": {
            "cefr_sp": [str(p) for p in discover_cefr_sp_tabular_files(RAW_CEFR_SP_DIR)],
            "cefrj_required": {k: str(v) for k, v in required_cefrj_paths(RAW_CEFRJ_DIR).items()},
            "cefrj_optional": {k: str(v) for k, v in optional_cefrj_paths(RAW_CEFRJ_DIR).items()},
            "ednoda_snapshot": [str(p) for p in discover_ednoda_snapshot_files(RAW_EDNODA_DIR)],
        },
        "processed_files_written": [str(p) for p in sorted(processed.glob("*.parquet"))],
        "row_counts": {
            "sentences": _row_count(processed / "sentences.parquet"),
            "vocabulary_reference": _row_count(processed / "vocabulary_reference.parquet"),
            "grammar_reference": _row_count(processed / "grammar_reference.parquet"),
            "source_registry": _row_count(processed / "source_registry.parquet"),
            "licenses": _row_count(processed / "licenses.parquet"),
        },
        "warnings": warnings,
    }
    (processed / "build_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[BUILD] Build completed. Manifest: {processed / 'build_manifest.json'}")


if __name__ == "__main__":
    main()
