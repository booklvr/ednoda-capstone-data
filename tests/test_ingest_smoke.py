from pathlib import Path

from ednoda_capstone_data.ingest import (
    ingest_cefr_sp,
    ingest_cefrj_grammar,
    ingest_cefrj_vocabulary,
    ingest_ednoda_snapshot,
)

FIXTURES = Path("tests/fixtures")


def test_cefr_sp_ingest_smoke_and_split():
    df = ingest_cefr_sp(FIXTURES / "cefr_sp")
    assert len(df) == 2
    assert set(df["cefr_level"].dropna().tolist()) == {"A1", "B1"}
    assert all(df["split"] == "train")


def test_cefrj_ingest_smoke():
    vdf = ingest_cefrj_vocabulary(
        FIXTURES / "cefrj/cefrj-vocabulary-profile-1.5.csv",
        FIXTURES / "cefrj/octanove-vocabulary-profile-c1c2-1.0.csv",
    )
    gdf = ingest_cefrj_grammar(FIXTURES / "cefrj/cefrj-grammar-profile-20180315.csv")
    assert len(vdf) == 3
    assert set(vdf["list_name"]) == {"cefrj_vocabulary_profile", "octanove_c1c2"}
    assert len(gdf) == 1 and gdf.iloc[0]["framework"] == "CEFR-J"


def test_ednoda_ingest_smoke():
    df = ingest_ednoda_snapshot(FIXTURES / "ednoda_snapshot")
    assert len(df) == 1
    assert df.iloc[0]["source_dataset"] == "ednoda_snapshot"
    assert df.iloc[0]["topic_hint"] == "school"
