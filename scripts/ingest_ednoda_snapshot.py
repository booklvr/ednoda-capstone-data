#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path

from ednoda_capstone_data.ingest import ingest_ednoda_snapshot


def main() -> None:
    snapshot_dir = Path("data/raw/ednoda_snapshot")
    out_path = Path("data/interim/ednoda_sentences.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ingest_ednoda_snapshot(snapshot_dir).to_parquet(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
