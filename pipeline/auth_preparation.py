from __future__ import annotations

from pathlib import Path
import pandas as pd

from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df


def prepare_auth_dataframe(path: str | Path) -> pd.DataFrame:
    raw_df = load_auth_sample(path)
    normalized_df = normalize_auth_df(raw_df)
    return normalized_df