#!/usr/bin/env python
"""Download CEFR-J files into data/raw/cefrj."""

from __future__ import annotations

import argparse
from pathlib import Path

import requests


def download(url: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, timeout=60, stream=True) as r:
        r.raise_for_status()
        with output.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vocabulary-url", required=True)
    parser.add_argument("--grammar-url", required=True)
    parser.add_argument("--c1c2-url")
    args = parser.parse_args()

    raw = Path("data/raw/cefrj")
    download(args.vocabulary_url, raw / "cefrj-vocabulary-profile-1.5.csv")
    download(args.grammar_url, raw / "cefrj-grammar-profile-20180315.csv")
    if args.c1c2_url:
        download(args.c1c2_url, raw / "octanove-vocabulary-profile-c1c2-1.0.csv")


if __name__ == "__main__":
    main()
