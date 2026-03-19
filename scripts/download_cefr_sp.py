#!/usr/bin/env python
"""Download CEFR-SP data archive/file into data/raw/cefr_sp."""

from __future__ import annotations

import argparse
from pathlib import Path

import requests


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--output", default="data/raw/cefr_sp/cefr_sp_download.bin")
    args = parser.parse_args()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(args.url, timeout=60, stream=True) as r:
        r.raise_for_status()
        with out_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    print(f"Saved CEFR-SP artifact to {out_path}")


if __name__ == "__main__":
    main()
