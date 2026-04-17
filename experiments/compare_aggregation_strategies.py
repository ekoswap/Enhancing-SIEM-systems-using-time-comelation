from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events
from lanl_deduplicate import deduplicate_auth_events
from lanl_temporal_aggregate import (
    temporal_aggregate_auth_events,
    temporal_aggregate_auth_events_relaxed,
)

auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
normalized_df = normalize_auth_df(auth_df)
filtered_df = filter_auth_events(normalized_df)
deduplicated_df = deduplicate_auth_events(filtered_df)

strict_df = temporal_aggregate_auth_events(deduplicated_df, time_window=5)
relaxed_df = temporal_aggregate_auth_events_relaxed(deduplicated_df, time_window=5)

print("Original:", len(deduplicated_df))

print("\nSTRICT AGGREGATION")
print("After:", len(strict_df))
print("Reduced:", len(deduplicated_df) - len(strict_df))
print("Top event_count values:")
print(strict_df["event_count"].value_counts().sort_index().head(10))

print("\nRELAXED AGGREGATION")
print("After:", len(relaxed_df))
print("Reduced:", len(deduplicated_df) - len(relaxed_df))
print("Top event_count values:")
print(relaxed_df["event_count"].value_counts().sort_index().head(10))