from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events
from lanl_deduplicate import deduplicate_auth_events
from lanl_temporal_aggregate import temporal_aggregate_auth_events

auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
normalized_df = normalize_auth_df(auth_df)
filtered_df = filter_auth_events(normalized_df)
deduplicated_df = deduplicate_auth_events(filtered_df)
aggregated_df = temporal_aggregate_auth_events(deduplicated_df, time_window=5)

print("Before aggregation:", len(deduplicated_df))
print("After aggregation:", len(aggregated_df))
print("Reduced by aggregation:", len(deduplicated_df) - len(aggregated_df))

print("\nTop aggregated event types:")
print(aggregated_df["event_type"].value_counts().head(10))

print("\nTop grouped event counts:")
print(aggregated_df["event_count"].value_counts().head(10))

print("\nFirst 10 aggregated rows:")
print(aggregated_df.head(10))