import pandas as pd

from ednoda_capstone_data.io_utils import ensure_columns
from ednoda_capstone_data.schemas import SENTENCES_COLUMNS


def test_schema_conformance_sentences():
    df = pd.DataFrame([{"text": "abc"}])
    conformed = ensure_columns(df, SENTENCES_COLUMNS)
    assert list(conformed.columns) == SENTENCES_COLUMNS
