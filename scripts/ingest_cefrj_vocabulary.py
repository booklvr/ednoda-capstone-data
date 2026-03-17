#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path

from ednoda_capstone_data.ingest import ingest_cefrj_vocabulary


def main() -> None:
    raw = Path("data/raw/cefrj")
    out_path = Path("data/interim/cefrj_vocabulary.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = ingest_cefrj_vocabulary(
        raw / "cefrj-vocabulary-profile-1.5.csv",
        raw / "octanove-vocabulary-profile-c1c2-1.0.csv",
    )
    df.to_parquet(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
