from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df

auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
normalized_df = normalize_auth_df(auth_df)

print("NORMALIZED AUTH SAMPLE:")
print(normalized_df.head())

print("\nColumns:")
print(normalized_df.columns.tolist())

print("\nShape:", normalized_df.shape)
print("Range:", normalized_df["timestamp"].min(), "->", normalized_df["timestamp"].max())

print("\nEvent type examples:")
print(normalized_df["event_type"].value_counts().head(10))