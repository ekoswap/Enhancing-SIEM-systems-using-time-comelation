from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df
from lanl_deduplicate import deduplicate_auth_events
from lanl_temporal_aggregate import temporal_aggregate_auth_events_relaxed
from lanl_baseline_correlation import (
    baseline_correlation_raw,
    baseline_correlation_aggregated,
)

# ---------------------------
# Old filter version
# ---------------------------
def filter_auth_events_old(df):
    working_df = df.copy()
    working_df = working_df.dropna(subset=["timestamp"])
    working_df = working_df[working_df["event_type"] != "ScreenLock_Success"]
    working_df = working_df.reset_index(drop=True)
    return working_df


# ---------------------------
# New improved filter version
# ---------------------------
def filter_auth_events_new(df):
    working_df = df.copy()
    working_df = working_df.dropna(subset=["timestamp"])

    excluded_event_types = {
        "ScreenLock_Success",
        "AuthMap_Success",
    }
    working_df = working_df[~working_df["event_type"].isin(excluded_event_types)]

    working_df = working_df[
        ~working_df["src_user"].astype(str).str.startswith("ANONYMOUS LOGON")
    ]

    working_df = working_df.reset_index(drop=True)
    return working_df


auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
normalized_df = normalize_auth_df(auth_df)

for version_name, filter_fn in [
    ("OLD_FILTER", filter_auth_events_old),
    ("NEW_FILTER", filter_auth_events_new),
]:
    filtered_df = filter_fn(normalized_df)
    dedup_df = deduplicate_auth_events(filtered_df)
    aggregated_df = temporal_aggregate_auth_events_relaxed(dedup_df, time_window=5)

    before_corr_df = baseline_correlation_raw(dedup_df, time_window=5)
    after_corr_df = baseline_correlation_aggregated(aggregated_df, time_window=5)

    event_reduction = len(dedup_df) - len(aggregated_df)
    event_reduction_pct = (event_reduction / len(dedup_df) * 100) if len(dedup_df) else 0

    corr_change = len(after_corr_df) - len(before_corr_df)
    corr_change_pct = (corr_change / len(before_corr_df) * 100) if len(before_corr_df) else 0

    print("=" * 60)
    print(version_name)
    print("=" * 60)
    print("After filter:", len(filtered_df))
    print("After dedup:", len(dedup_df))
    print("After aggregation:", len(aggregated_df))
    print(f"Aggregation reduction: {event_reduction} ({event_reduction_pct:.2f}%)")
    print("Correlation before:", len(before_corr_df))
    print("Correlation after:", len(after_corr_df))
    print(f"Correlation change: {corr_change} ({corr_change_pct:.2f}%)")
    print()