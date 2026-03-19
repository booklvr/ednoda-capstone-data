# Dataset Acquisition

This guide answers a practical question for the capstone team: where do the datasets come from, and which ones can we fetch automatically?

## Recommended split

### Scripted now

These have stable public URLs and are now supported by `scripts/fetch_public_data.py`:

- `cefr-sp`
- `cefr-j`
- `ud-english-esl`

### Manual or optional for now

These are still worth using, but I recommend treating them as optional/manual until the team decides the exact subset and licensing posture:

- `wiki-auto`
- `universalcefr-en`
- `ednoda_snapshot` (internal export provided by Ednoda)

## One-command setup for the core public datasets

After activating the virtual environment:

```bash
python scripts/fetch_public_data.py core
python scripts/preflight_check.py
python scripts/build_all.py
```

To preview without downloading:

```bash
python scripts/fetch_public_data.py core --dry-run
```

To list available dataset keys:

```bash
python scripts/fetch_public_data.py --list
```

## Dataset-by-dataset notes

### 1. CEFR-SP

- Upstream repo: https://github.com/yukiar/CEFR-SP
- Repo note: the corpus lives in the `/CEFR-SP` directory
- Current scripted behavior:
  - downloads the public GitHub repository archive
  - extracts the `/CEFR-SP` folder into `data/raw/cefr_sp/`
- Local destination:
  - `data/raw/cefr_sp/`

Scripted command:

```bash
python scripts/fetch_public_data.py cefr-sp
```

Important note:

- The upstream repo clearly exposes the corpus publicly, but this repo still treats redistribution conservatively until license terms are explicitly verified.

### 2. CEFR-J

- Upstream repo: https://github.com/openlanguageprofiles/olp-en-cefrj
- Current scripted files:
  - `cefrj-vocabulary-profile-1.5.csv`
  - `cefrj-grammar-profile-20180315.csv`
  - `octanove-vocabulary-profile-c1c2-1.0.csv`
- Local destination:
  - `data/raw/cefrj/`

Scripted command:

```bash
python scripts/fetch_public_data.py cefr-j
```

Upstream terms summary:

- The repository README says the CEFR-J vocabulary and grammar profile datasets can be used for research and commercial purposes with citation.
- The Octanove C1/C2 vocabulary file is under CC BY-SA 4.0.

### 3. UD English-ESL

- Upstream repo: https://github.com/UniversalDependencies/UD_English-ESL
- Current scripted files:
  - `en_esl-ud-train.conllu`
  - `en_esl-ud-dev.conllu`
  - `en_esl-ud-test.conllu`
  - `LICENSE.txt`
- Local destination:
  - `data/raw/ud_english_esl/`

Scripted command:

```bash
python scripts/fetch_public_data.py ud-english-esl
```

Important note:

- The repository is public under CC BY-SA 4.0.
- However, the project README says reconstructing the original learner text requires separate FCE licensing. For this capstone, treat UD English-ESL primarily as a grammar/treebank resource, not as the main elementary sentence pool.

### 4. Wiki-Auto

- Suggested source: Hugging Face dataset card
  - https://huggingface.co/datasets/chaojiang06/wiki_auto
- Suggested local destination:
  - `data/raw/wiki_auto/`

Recommendation:

- Keep this optional for the initial handoff.
- The dataset is useful for simplification experiments, but the exact config/subset should be chosen deliberately by the student team.

Suggested manual workflow:

1. Review the dataset card and selected config.
2. Download only the subset you want to experiment with.
3. Save the raw files under `data/raw/wiki_auto/`.
4. Record the exact source URL, config, and license in project notes.

### 5. UniversalCEFR English subsets

- UniversalCEFR organization: https://huggingface.co/UniversalCEFR
- Dataset listing: https://huggingface.co/UniversalCEFR/datasets
- Example English datasets currently listed there include:
  - `UniversalCEFR/cefr_sp_en`
  - `UniversalCEFR/elg_cefr_en`
  - `UniversalCEFR/cambridge_exams_en`
  - `UniversalCEFR/icle500_en`
  - `UniversalCEFR/readme_en`

Recommendation:

- Treat this as optional and selective.
- Only pull the English subsets that fit the project and whose original licenses are acceptable.
- Preserve source-level license metadata because different UniversalCEFR subsets inherit different licenses from their original datasets.

## What I recommend for Ednoda right now

If the goal is to get the repo student-ready quickly:

1. Use the scripted `core` download path immediately.
2. Build the processed v1 tables from CEFR-SP + CEFR-J.
3. Keep UD English-ESL available as a syntax/treebank side resource.
4. Add Wiki-Auto only after the core pipeline is working.
5. Treat UniversalCEFR as a later extension, not a day-one dependency.
