# Onboarding

This page is written for a new contributor joining the project for the first time.

## 1. Set up Python

Use Python 3.10+.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

Note: on some machines `python` is unavailable until the virtual environment is activated, so prefer starting with `python3`.

## 2. Install dependencies

```bash
python -m pip install -r requirements.txt
```

Optional editable install:

```bash
python -m pip install --no-build-isolation -e .[dev]
```

## 3. Verify the repo itself

```bash
python -m pytest -q
```

At this stage, tests should pass even if you have not added any datasets yet.

## 4. Fetch the recommended day-one datasets

Use the scripted fetch path for the default capstone setup:

```bash
python scripts/fetch_public_data.py cefr-sp cefr-j
python scripts/preflight_check.py
```

This gives students the current recommended starting point:

- `CEFR-SP`
- `CEFR-J` vocabulary
- `CEFR-J` grammar

The preflight check should now report CEFR-SP and CEFR-J as present.

## 5. Optional: place raw files manually

- Required:
  - `data/raw/cefr_sp/`
  - `data/raw/cefrj/` (required exact CEFR-J filenames)
- Optional:
  - `data/raw/ednoda_snapshot/`

Manual placement is still supported, but the fetch script is preferred for the default public datasets.

## 6. Run the full build

```bash
python scripts/build_all.py
```

You can rerun this safely.

- The build overwrites the generated parquet outputs.
- It does not append duplicate rows just because you ran it more than once.
- If the raw input data changes, the rebuilt outputs may change too.

## 7. Review outputs

```bash
python scripts/describe_processed_data.py
```

- `data/processed/day1_summary.md`
- `data/processed/validation_summary.json`
- `data/processed/build_manifest.json`
- processed parquet files in `data/processed/`
