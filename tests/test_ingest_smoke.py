from pathlib import Path

from ednoda_capstone_data.ingest import ingest_cefr_sp, ingest_cefrj_grammar, ingest_cefrj_vocabulary


def test_cefr_sp_ingest_smoke(tmp_path: Path):
    raw = tmp_path / "CEFR-SP"
    raw.mkdir()
    (raw / "sample.csv").write_text("sentence,cefr\nI am here.,A1\n", encoding="utf-8")
    df = ingest_cefr_sp(raw)
    assert len(df) == 1
    assert df.iloc[0]["cefr_level"] == "A1"


def test_cefrj_ingest_smoke(tmp_path: Path):
    vocab = tmp_path / "cefrj-vocabulary-profile-1.5.csv"
    vocab.write_text("word,cefr,pos\nrun,B1,verb\n", encoding="utf-8")
    grammar = tmp_path / "cefrj-grammar-profile-20180315.csv"
    grammar.write_text("category,level,pattern\nTense,A2,past simple\n", encoding="utf-8")
    vdf = ingest_cefrj_vocabulary(vocab)
    gdf = ingest_cefrj_grammar(grammar)
    assert len(vdf) == 1 and vdf.iloc[0]["cefr_level"] == "B1"
    assert len(gdf) == 1 and gdf.iloc[0]["cefr_level"] == "A2"
