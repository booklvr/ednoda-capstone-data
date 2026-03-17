#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path

from ednoda_capstone_data.ingest import ingest_cefr_sp


def main() -> None:
    raw_dir = Path("data/raw/cefr_sp")
    out_path = Path("data/interim/cefr_sp_sentences.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ingest_cefr_sp(raw_dir).to_parquet(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
