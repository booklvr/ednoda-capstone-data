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
    assert vdf.iloc[0]["headword"] == "run"
    assert vdf.iloc[0]["cefr_level"] == "B1"
    assert gdf.iloc[0]["grammar_item"] == "past simple"
    assert gdf.iloc[0]["cefr_level"] == "A2"


def test_ednoda_ingest_smoke():
    df = ingest_ednoda_snapshot(FIXTURES / "ednoda_snapshot")
    assert len(df) == 1
    assert df.iloc[0]["source_dataset"] == "ednoda_snapshot"
    assert df.iloc[0]["topic_hint"] == "school"


def test_ednoda_ingest_preserves_export_metadata(tmp_path):
    raw_dir = tmp_path / "ednoda_snapshot"
    raw_dir.mkdir(parents=True)
    (raw_dir / "snapshot.csv").write_text(
        "education_node_id,text,node_type,textbook,unit,region,grade\n"
        "42,This is Ednoda.,sentence,Oxford Grade 3,5,Korea,3\n",
        encoding="utf-8",
    )

    df = ingest_ednoda_snapshot(raw_dir)

    assert len(df) == 1
    assert df.iloc[0]["record_id"] == "ednoda_snapshot::42"
    assert df.iloc[0]["source_record_id"] == "42"
    assert df.iloc[0]["grade_hint"] == 3 or str(df.iloc[0]["grade_hint"]) == "3"
    assert df.iloc[0]["region_hint"] == "Korea"
    assert "Oxford Grade 3" in df.iloc[0]["metadata_json"]


def test_cefr_sp_headerless_txt_ingest(tmp_path):
    raw_dir = tmp_path / "cefr_sp"
    score_dir = raw_dir / "SCoRE"
    score_dir.mkdir(parents=True)
    (score_dir / "CEFR-SP_SCoRE_train.txt").write_text(
        "Is that your bike ?\t1\t1\nBen must like his new car a lot .\t1\t2\n",
        encoding="utf-8",
    )

    df = ingest_cefr_sp(raw_dir)

    assert len(df) == 2
    assert df.iloc[0]["text_normalized"] == "Is that your bike ?"
    assert df.iloc[0]["cefr_level"] == "A1"
    assert df.iloc[1]["cefr_level"] == "A2"
