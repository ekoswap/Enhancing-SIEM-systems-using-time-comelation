from __future__ import annotations

import pandas as pd

from pipeline.common import aggregate_with_max_flag


PROC_COLUMNS = [
    "timestamp",
    "user",
    "computer",
    "process",
    "event_state",
]


def proc_filter_end_only(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["event_state"] == "End"].copy()


def proc_best_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    return aggregate_with_max_flag(
        df=df,
        group_cols=["computer", "process"],
        time_col="timestamp",
    )