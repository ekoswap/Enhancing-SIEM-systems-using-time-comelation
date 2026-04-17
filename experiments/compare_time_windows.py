from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events
from lanl_deduplicate import deduplicate_auth_events
from lanl_temporal_aggregate import temporal_aggregate_auth_events

auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
normalized_df = normalize_auth_df(auth_df)
filtered_df = filter_auth_events(normalized_df)
deduplicated_df = deduplicate_auth_events(filtered_df)

original_count = len(deduplicated_df)

print("Original event count:", original_count)
print("=" * 60)

for window in [1, 3, 5, 10, 20]:
    aggregated_df = temporal_aggregate_auth_events(deduplicated_df, time_window=window)
    reduced = original_count - len(aggregated_df)
    reduction_pct = (reduced / original_count) * 100 if original_count else 0

    grouped_counts = aggregated_df["event_count"].value_counts().sort_index()

    print(f"Time window = {window}")
    print(f"After aggregation: {len(aggregated_df)}")
    print(f"Reduced: {reduced}")
    print(f"Reduction %: {reduction_pct:.2f}%")
    print("Grouped event_count distribution:")
    print(grouped_counts.head(10))
    print("-" * 60)