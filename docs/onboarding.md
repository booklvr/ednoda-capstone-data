# Onboarding

1. Create and activate a Python 3.10+ environment.
2. Install dependencies with `pip install -r requirements.txt`.
3. Optional editable mode: `pip install --no-build-isolation -e .[dev]`.
4. Place raw files in expected paths:
   - `data/raw/cefr_sp/`
   - `data/raw/cefrj/` (required exact CEFR-J filenames)
   - `data/raw/ednoda_snapshot/` (optional)
5. Run `python scripts/preflight_check.py`.
6. Run tests: `PYTHONPATH=src pytest -q`.
7. Run full build: `python scripts/build_all.py`.
8. Review `data/processed/validation_summary.json` and `data/processed/build_manifest.json`.
