from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events

auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
normalized_df = normalize_auth_df(auth_df)
filtered_df = filter_auth_events(normalized_df)

print("Before filtering:", len(normalized_df))
print("After filtering:", len(filtered_df))
print("Removed:", len(normalized_df) - len(filtered_df))

print("\nRemaining event types:")
print(filtered_df["event_type"].value_counts())