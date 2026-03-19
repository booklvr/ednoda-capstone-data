#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ednoda_capstone_data.ingest import ingest_cefrj_vocabulary
from ednoda_capstone_data.sources import CEFRJ_C1C2_FILE, CEFRJ_VOCAB_FILE, RAW_CEFRJ_DIR


def _find_fallback_csv(raw_dir: Path, keyword: str) -> Path | None:
    matches = sorted([p for p in raw_dir.glob("*.csv") if keyword in p.name.lower()])
    return matches[0] if matches else None


def main() -> None:
    out_path = Path("data/interim/cefrj_vocabulary.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    vocab_path = RAW_CEFRJ_DIR / CEFRJ_VOCAB_FILE
    c1c2_path = RAW_CEFRJ_DIR / CEFRJ_C1C2_FILE
    if not vocab_path.exists():
        fallback = _find_fallback_csv(RAW_CEFRJ_DIR, "vocab")
        if fallback:
            print(f"[WARN] Expected vocab file missing. Falling back to {fallback}")
            vocab_path = fallback

    df = ingest_cefrj_vocabulary(vocab_path, c1c2_path if c1c2_path.exists() else None)
    df.to_parquet(out_path, index=False)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
