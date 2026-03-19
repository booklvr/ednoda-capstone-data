"""Source manifests and raw-file discovery helpers."""

from __future__ import annotations

from pathlib import Path

RAW_CEFR_SP_DIR = Path("data/raw/cefr_sp")
RAW_CEFRJ_DIR = Path("data/raw/cefrj")
RAW_EDNODA_DIR = Path("data/raw/ednoda_snapshot")

CEFR_SP_UPSTREAM_REPO = "https://github.com/yukiar/CEFR-SP"
CEFR_SP_UPSTREAM_CORPUS_NOTE = "Upstream CEFR-SP corpus is expected under /CEFR-SP in the source repo."

CEFRJ_UPSTREAM_REPO = "https://github.com/openlanguageprofiles/olp-en-cefrj"
CEFRJ_PROJECT_SITE = "http://www.cefr-j.org/"

CEFRJ_VOCAB_FILE = "cefrj-vocabulary-profile-1.5.csv"
CEFRJ_GRAMMAR_FILE = "cefrj-grammar-profile-20180315.csv"
CEFRJ_C1C2_FILE = "octanove-vocabulary-profile-c1c2-1.0.csv"


def required_cefrj_paths(root: Path = RAW_CEFRJ_DIR) -> dict[str, Path]:
    return {
        "vocabulary": root / CEFRJ_VOCAB_FILE,
        "grammar": root / CEFRJ_GRAMMAR_FILE,
    }


def optional_cefrj_paths(root: Path = RAW_CEFRJ_DIR) -> dict[str, Path]:
    return {"octanove_c1c2": root / CEFRJ_C1C2_FILE}


def discover_cefr_sp_tabular_files(root: Path = RAW_CEFR_SP_DIR) -> list[Path]:
    if not root.exists():
        return []
    return sorted([p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in {".csv", ".tsv", ".txt", ".xlsx", ".xls"}])


def discover_ednoda_snapshot_files(root: Path = RAW_EDNODA_DIR) -> list[Path]:
    if not root.exists():
        return []
    return sorted(root.glob("*.csv"))
