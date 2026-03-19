from ednoda_capstone_data.normalize import cefr_to_numeric, normalize_cefr, normalize_text


def test_normalize_cefr_levels():
    assert normalize_cefr("a1") == "A1"
    assert normalize_cefr("Pre-A1") == "A1"
    assert normalize_cefr("B 2") == "B2"
    assert normalize_cefr("A1.1") == "A1"
    assert normalize_cefr("B2-1") == "B2"
    assert normalize_cefr("unknown") is None


def test_cefr_numeric_mapping():
    assert cefr_to_numeric("A1") == 1
    assert cefr_to_numeric("C2") == 6
    assert cefr_to_numeric("B 1") == 3
    assert cefr_to_numeric("invalid") is None


def test_normalize_text_basic():
    assert normalize_text(" Hello\u00a0 world   ") == "Hello world"
    assert normalize_text(None) == ""
