from ednoda_capstone_data.normalize import normalize_cefr, normalize_text


def test_normalize_cefr_levels():
    assert normalize_cefr("a1") == "A1"
    assert normalize_cefr("Pre-A1") == "A1"
    assert normalize_cefr("B 2") == "B2"
    assert normalize_cefr("unknown") is None


def test_normalize_text_basic():
    assert normalize_text(" Hello\u00a0 world   ") == "Hello world"
    assert normalize_text(None) == ""
