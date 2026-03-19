#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ednoda_capstone_data.ingest import ingest_cefrj_grammar
from ednoda_capstone_data.sources import CEFRJ_GRAMMAR_FILE, RAW_CEFRJ_DIR


def _find_fallback_csv(raw_dir: Path, keyword: str) -> Path | None:
    matches = sorted([p for p in raw_dir.glob("*.csv") if keyword in p.name.lower()])
    return matches[0] if matches else None


def main() -> None:
    path = RAW_CEFRJ_DIR / CEFRJ_GRAMMAR_FILE
    if not path.exists():
        fallback = _find_fallback_csv(RAW_CEFRJ_DIR, "grammar")
        if fallback:
            print(f"[WARN] Expected grammar file missing. Falling back to {fallback}")
            path = fallback

    out_path = Path("data/interim/cefrj_grammar.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ingest_cefrj_grammar(path).to_parquet(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
