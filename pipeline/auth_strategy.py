from __future__ import annotations

import pandas as pd

from lanl_filter import filter_auth_events
from lanl_temporal_aggregate import temporal_aggregate_auth_events_relaxed


AUTH_COLUMNS = [
    "timestamp",
    "src_user",
    "src_user_name",
    "src_user_domain",
    "dst_user",
    "dst_user_name",
    "dst_user_domain",
    "src_computer",
    "dst_computer",
    "auth_type",
    "logon_type",
    "auth_orientation",
    "result",
    "event_type",
]


def auth_filter_improved(df: pd.DataFrame) -> pd.DataFrame:
    return filter_auth_events(df)


def auth_exact_dedup(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates().copy()


def auth_best_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    return temporal_aggregate_auth_events_relaxed(df, time_window=5)