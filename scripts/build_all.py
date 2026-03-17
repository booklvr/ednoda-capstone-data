#!/usr/bin/env python
from __future__ import annotations

import subprocess
import sys

SCRIPTS = [
    "scripts/ingest_cefr_sp.py",
    "scripts/ingest_cefrj_vocabulary.py",
    "scripts/ingest_cefrj_grammar.py",
    "scripts/ingest_ednoda_snapshot.py",
    "scripts/merge_processed_tables.py",
    "scripts/validate_processed_tables.py",
]


def main() -> None:
    for script in SCRIPTS:
        print(f"Running {script}")
        subprocess.run([sys.executable, script], check=True)


if __name__ == "__main__":
    main()
