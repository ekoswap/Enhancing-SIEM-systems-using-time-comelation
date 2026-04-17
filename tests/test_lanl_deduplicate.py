from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events
from lanl_deduplicate import deduplicate_auth_events

auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
normalized_df = normalize_auth_df(auth_df)
filtered_df = filter_auth_events(normalized_df)
deduplicated_df = deduplicate_auth_events(filtered_df)

print("Before deduplication:", len(filtered_df))
print("After deduplication:", len(deduplicated_df))
print("Removed duplicates:", len(filtered_df) - len(deduplicated_df))

print("\nTop event types after deduplication:")
print(deduplicated_df["event_type"].value_counts().head(10))