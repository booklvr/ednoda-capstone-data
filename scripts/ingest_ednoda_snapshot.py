#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ednoda_capstone_data.ingest import ingest_ednoda_snapshot
from ednoda_capstone_data.sources import RAW_EDNODA_DIR


def main() -> None:
    out_path = Path("data/interim/ednoda_sentences.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ingest_ednoda_snapshot(RAW_EDNODA_DIR).to_parquet(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
