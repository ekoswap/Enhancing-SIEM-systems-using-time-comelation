from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events
from lanl_deduplicate import deduplicate_auth_events
from lanl_temporal_aggregate import temporal_aggregate_auth_events_relaxed
from lanl_baseline_correlation import (
    baseline_correlation_raw,
    baseline_correlation_aggregated,
)

auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
normalized_df = normalize_auth_df(auth_df)
filtered_df = filter_auth_events(normalized_df)
deduplicated_df = deduplicate_auth_events(filtered_df)
aggregated_df = temporal_aggregate_auth_events_relaxed(deduplicated_df, time_window=5)

before_corr_df = baseline_correlation_raw(deduplicated_df, time_window=5)
after_corr_df = baseline_correlation_aggregated(aggregated_df, time_window=5)

print("RAW events before correlation:", len(deduplicated_df))
print("Aggregated events before correlation:", len(aggregated_df))

print("\nCorrelation results BEFORE aggregation:", len(before_corr_df))
print("Correlation results AFTER aggregation:", len(after_corr_df))

if len(before_corr_df) > 0:
    change = len(after_corr_df) - len(before_corr_df)
    pct = (change / len(before_corr_df)) * 100
    print(f"Change in correlation result count: {change} ({pct:.2f}%)")
else:
    print("Change in correlation result count: N/A")

print("\nTop BEFORE event types:")
if not before_corr_df.empty:
    print(before_corr_df["event_type"].value_counts().head(10))
else:
    print("No results")

print("\nTop AFTER event types:")
if not after_corr_df.empty:
    print(after_corr_df["event_type"].value_counts().head(10))
else:
    print("No results")