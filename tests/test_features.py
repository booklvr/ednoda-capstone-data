from ednoda_capstone_data.features import compute_surface_features, lexical_density_from_pos, safe_divide, surface_tokens


def test_surface_tokens_basic():
    assert surface_tokens("Hello, world! It's fine.") == ["Hello", "world", "It's", "fine"]


def test_safe_divide_handles_zero():
    assert safe_divide(3, 0) is None
    assert safe_divide(4, 2) == 2.0


def test_lexical_density_from_pos():
    alpha_count, content_count, lexical_density = lexical_density_from_pos(["PRON", "VERB", "DET", "NOUN"])
    assert alpha_count == 4
    assert content_count == 2
    assert lexical_density == 0.5


def test_compute_surface_features():
    features = compute_surface_features("Cats chase mice.")
    assert features["surface_token_count"] == 3
    assert features["surface_char_count"] == len("Cats chase mice.")
