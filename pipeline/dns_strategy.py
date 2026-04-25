from __future__ import annotations

import pandas as pd

from pipeline.common import aggregate_with_max_flag


DNS_COLUMNS = [
    "time",
    "source_computer",
    "destination_computer",
]


def dns_best_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    return aggregate_with_max_flag(
        df=df,
        group_cols=["source_computer", "destination_computer"],
        time_col="time",
    )