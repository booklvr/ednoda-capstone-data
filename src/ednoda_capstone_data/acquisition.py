"""Dataset acquisition metadata for public sources."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DownloadFile:
    url: str
    relative_path: str


@dataclass(frozen=True)
class DatasetSpec:
    key: str
    title: str
    mode: str
    target_dir: str
    files: tuple[DownloadFile, ...] = ()
    archive_url: str | None = None
    extract_subdir: str | None = None
    notes: str = ""


DATASET_SPECS: dict[str, DatasetSpec] = {
    "cefr-sp": DatasetSpec(
        key="cefr-sp",
        title="CEFR-SP",
        mode="scripted",
        target_dir="data/raw/cefr_sp",
        archive_url="https://codeload.github.com/yukiar/CEFR-SP/zip/refs/heads/main",
        extract_subdir="CEFR-SP-main/CEFR-SP",
        notes="Source repo exposes CEFR-SP under the /CEFR-SP directory. License should still be verified before redistribution.",
    ),
    "cefr-j": DatasetSpec(
        key="cefr-j",
        title="CEFR-J (Open Language Profiles)",
        mode="scripted",
        target_dir="data/raw/cefrj",
        files=(
            DownloadFile(
                url="https://raw.githubusercontent.com/openlanguageprofiles/olp-en-cefrj/master/cefrj-vocabulary-profile-1.5.csv",
                relative_path="cefrj-vocabulary-profile-1.5.csv",
            ),
            DownloadFile(
                url="https://raw.githubusercontent.com/openlanguageprofiles/olp-en-cefrj/master/cefrj-grammar-profile-20180315.csv",
                relative_path="cefrj-grammar-profile-20180315.csv",
            ),
            DownloadFile(
                url="https://raw.githubusercontent.com/openlanguageprofiles/olp-en-cefrj/master/octanove-vocabulary-profile-c1c2-1.0.csv",
                relative_path="octanove-vocabulary-profile-c1c2-1.0.csv",
            ),
        ),
        notes="CEFR-J terms allow research/commercial use with citation according to the upstream repository README.",
    ),
    "ud-english-esl": DatasetSpec(
        key="ud-english-esl",
        title="UD English-ESL",
        mode="scripted",
        target_dir="data/raw/ud_english_esl",
        files=(
            DownloadFile(
                url="https://raw.githubusercontent.com/UniversalDependencies/UD_English-ESL/master/en_esl-ud-train.conllu",
                relative_path="en_esl-ud-train.conllu",
            ),
            DownloadFile(
                url="https://raw.githubusercontent.com/UniversalDependencies/UD_English-ESL/master/en_esl-ud-dev.conllu",
                relative_path="en_esl-ud-dev.conllu",
            ),
            DownloadFile(
                url="https://raw.githubusercontent.com/UniversalDependencies/UD_English-ESL/master/en_esl-ud-test.conllu",
                relative_path="en_esl-ud-test.conllu",
            ),
            DownloadFile(
                url="https://raw.githubusercontent.com/UniversalDependencies/UD_English-ESL/master/LICENSE.txt",
                relative_path="LICENSE.txt",
            ),
        ),
        notes="Treebank annotations are public under CC BY-SA 4.0, but the repository notes that original learner text requires separate FCE licensing to fully reconstruct.",
    ),
    "wiki-auto": DatasetSpec(
        key="wiki-auto",
        title="Wiki-Auto",
        mode="manual",
        target_dir="data/raw/wiki_auto",
        notes="Recommended acquisition path is Hugging Face. Treat as optional and verify the exact subset/config you want before automating.",
    ),
    "universalcefr-en": DatasetSpec(
        key="universalcefr-en",
        title="UniversalCEFR English subsets",
        mode="manual",
        target_dir="data/raw/universalcefr",
        notes="Recommended acquisition path is the Hugging Face UniversalCEFR organization. Select only the English datasets relevant to the capstone and keep original license metadata.",
    ),
}


ALIASES: dict[str, tuple[str, ...]] = {
    "core": ("cefr-sp", "cefr-j", "ud-english-esl"),
    "all-scripted": ("cefr-sp", "cefr-j", "ud-english-esl"),
}


def expand_dataset_names(names: list[str]) -> list[DatasetSpec]:
    expanded: list[str] = []
    for name in names:
        if name in ALIASES:
            expanded.extend(ALIASES[name])
        else:
            expanded.append(name)

    specs: list[DatasetSpec] = []
    seen: set[str] = set()
    for key in expanded:
        if key not in DATASET_SPECS:
            known = ", ".join(sorted(DATASET_SPECS))
            raise ValueError(f"Unknown dataset key '{key}'. Known dataset keys: {known}")
        if key in seen:
            continue
        seen.add(key)
        specs.append(DATASET_SPECS[key])
    return specs
