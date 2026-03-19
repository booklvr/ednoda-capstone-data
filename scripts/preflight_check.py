#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import argparse
import json

from ednoda_capstone_data.sources import (
    RAW_CEFRJ_DIR,
    RAW_CEFR_SP_DIR,
    RAW_EDNODA_DIR,
    discover_cefr_sp_tabular_files,
    discover_ednoda_snapshot_files,
    optional_cefrj_paths,
    required_cefrj_paths,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--for-build", action="store_true", help="Exit non-zero when required v1 sources are missing")
    args = parser.parse_args()

    for path in [RAW_CEFR_SP_DIR, RAW_CEFRJ_DIR, RAW_EDNODA_DIR, Path("data/interim"), Path("data/processed")]:
        path.mkdir(parents=True, exist_ok=True)

    cefr_sp_files = discover_cefr_sp_tabular_files(RAW_CEFR_SP_DIR)
    cefrj_required = required_cefrj_paths(RAW_CEFRJ_DIR)
    cefrj_optional = optional_cefrj_paths(RAW_CEFRJ_DIR)
    ednoda_files = discover_ednoda_snapshot_files(RAW_EDNODA_DIR)

    status = {
        "cefr_sp_present": len(cefr_sp_files) > 0,
        "cefr_sp_file_count": len(cefr_sp_files),
        "cefrj_vocab_present": cefrj_required["vocabulary"].exists(),
        "cefrj_grammar_present": cefrj_required["grammar"].exists(),
        "cefrj_optional_c1c2_present": cefrj_optional["octanove_c1c2"].exists(),
        "ednoda_snapshot_present": len(ednoda_files) > 0,
    }

    print("Preflight readiness report")
    print(f"- CEFR-SP present: {status['cefr_sp_present']} ({status['cefr_sp_file_count']} tabular files found)")
    print(f"- CEFR-J vocabulary present: {status['cefrj_vocab_present']} ({cefrj_required['vocabulary']})")
    print(f"- CEFR-J grammar present: {status['cefrj_grammar_present']} ({cefrj_required['grammar']})")
    print(f"- Ednoda snapshot present (optional): {status['ednoda_snapshot_present']}")

    manifest_path = Path("data/processed/preflight_status.json")
    manifest_path.write_text(json.dumps(status, indent=2), encoding="utf-8")

    missing_required = not (status["cefr_sp_present"] and status["cefrj_vocab_present"] and status["cefrj_grammar_present"])
    if args.for_build and missing_required:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
