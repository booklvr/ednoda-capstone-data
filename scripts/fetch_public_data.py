#!/usr/bin/env python
"""Download public capstone datasets into data/raw."""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import zipfile

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import argparse

import requests

from ednoda_capstone_data.acquisition import DATASET_SPECS, expand_dataset_names


def _download_file(url: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, timeout=120, stream=True) as response:
        response.raise_for_status()
        with output_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)


def _download_and_extract_subdir(url: str, subdir: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp_dir:
        archive_path = Path(temp_dir) / "archive.zip"
        _download_file(url, archive_path)
        with zipfile.ZipFile(archive_path) as archive:
            members = [name for name in archive.namelist() if name.startswith(f"{subdir}/") and not name.endswith("/")]
            if not members:
                raise FileNotFoundError(f"Could not find subdir '{subdir}' inside archive from {url}")
            for member in members:
                relative = Path(member).relative_to(subdir)
                destination = output_dir / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, destination.open("wb") as target:
                    target.write(source.read())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch public Ednoda capstone datasets that have stable scripted download paths."
    )
    parser.add_argument(
        "datasets",
        nargs="*",
        default=["core"],
        help="Dataset keys or aliases. Examples: core, cefr-sp, cefr-j, ud-english-esl, wiki-auto, universalcefr-en",
    )
    parser.add_argument("--list", action="store_true", help="List known datasets and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded without downloading.")
    args = parser.parse_args()

    if args.list:
        print("Known dataset keys:")
        for key in sorted(DATASET_SPECS):
            spec = DATASET_SPECS[key]
            print(f"- {spec.key}: {spec.title} [{spec.mode}] -> {spec.target_dir}")
        print("Aliases: core, all-scripted")
        return

    specs = expand_dataset_names(args.datasets)

    for spec in specs:
        print(f"[DATASET] {spec.title} ({spec.key})")
        print(f"  mode: {spec.mode}")
        print(f"  target_dir: {spec.target_dir}")
        if spec.notes:
            print(f"  notes: {spec.notes}")

        if spec.mode != "scripted":
            print("  action: manual acquisition required")
            continue

        if spec.archive_url and spec.extract_subdir:
            if args.dry_run:
                print(f"  would download archive: {spec.archive_url}")
                print(f"  would extract subdir: {spec.extract_subdir}")
                print(f"  to: {spec.target_dir}")
                continue
            print(f"  downloading archive: {spec.archive_url}")
            _download_and_extract_subdir(spec.archive_url, spec.extract_subdir, Path(spec.target_dir))
            print(f"  extracted to: {spec.target_dir}")
            continue

        for file_spec in spec.files:
            output_path = Path(spec.target_dir) / file_spec.relative_path
            if args.dry_run:
                print(f"  would download: {file_spec.url}")
                print(f"  to: {output_path}")
                continue

            print(f"  downloading: {file_spec.url}")
            _download_file(file_spec.url, output_path)
            print(f"  saved: {output_path}")


if __name__ == "__main__":
    main()
