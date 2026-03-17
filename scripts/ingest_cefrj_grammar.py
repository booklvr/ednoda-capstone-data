#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path

from ednoda_capstone_data.ingest import ingest_cefrj_grammar


def main() -> None:
    path = Path("data/raw/cefrj/cefrj-grammar-profile-20180315.csv")
    out_path = Path("data/interim/cefrj_grammar.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ingest_cefrj_grammar(path).to_parquet(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
