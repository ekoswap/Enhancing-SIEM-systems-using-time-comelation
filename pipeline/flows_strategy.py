from __future__ import annotations

import pandas as pd

from pipeline.common import aggregate_with_max_flag


FLOWS_COLUMNS = [
    "time",
    "duration",
    "source_computer",
    "source_port",
    "destination_computer",
    "destination_port",
    "protocol",
    "packet_count",
    "byte_count",
]


def flows_filter_duration_only(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["duration"] > 0].copy()


def flows_best_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    return aggregate_with_max_flag(
        df=df,
        group_cols=["destination_computer", "protocol"],
        time_col="time",
        extra_aggs={
            "total_packets": ("packet_count", "sum"),
            "total_bytes": ("byte_count", "sum"),
        },
    )