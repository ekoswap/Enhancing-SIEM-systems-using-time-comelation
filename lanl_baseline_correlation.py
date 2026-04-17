import pandas as pd


def baseline_correlation_raw(df: pd.DataFrame, time_window: int = 5) -> pd.DataFrame:
    """
    Faster baseline correlation on raw auth events.
    Group by:
    - src_user
    - dst_computer
    - event_type
    Then summarize only groups that have at least 2 events
    and occur within the allowed time span.
    """
    working_df = df.copy()

    grouped = (
        working_df
        .groupby(["src_user", "dst_computer", "event_type"], dropna=False)
        .agg(
            start_time=("timestamp", "min"),
            end_time=("timestamp", "max"),
            event_count=("timestamp", "size"),
        )
        .reset_index()
    )

    correlated_df = grouped[
        (grouped["event_count"] >= 2) &
        ((grouped["end_time"] - grouped["start_time"]) <= time_window)
    ].copy()

    correlated_df = correlated_df.reset_index(drop=True)
    correlated_df.insert(0, "correlation_id", range(1, len(correlated_df) + 1))

    return correlated_df


def baseline_correlation_aggregated(df: pd.DataFrame, time_window: int = 5) -> pd.DataFrame:
    """
    Faster baseline correlation on aggregated auth events.
    Group by:
    - src_user
    - dst_computer
    - event_type
    Then summarize only groups that have at least 2 grouped events
    and occur within the allowed time span.
    """
    working_df = df.copy()

    grouped = (
        working_df
        .groupby(["src_user", "dst_computer", "event_type"], dropna=False)
        .agg(
            start_time=("start_time", "min"),
            end_time=("end_time", "max"),
            group_count=("group_id", "size"),
            underlying_event_count=("event_count", "sum"),
        )
        .reset_index()
    )

    correlated_df = grouped[
        (grouped["group_count"] >= 2) &
        ((grouped["end_time"] - grouped["start_time"]) <= time_window)
    ].copy()

    correlated_df = correlated_df.reset_index(drop=True)
    correlated_df.insert(0, "correlation_id", range(1, len(correlated_df) + 1))

    return correlated_df