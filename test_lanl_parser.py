from lanl_parser import load_auth_sample, load_redteam

auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
redteam_df = load_redteam("data/raw/redteam.txt")

print("AUTH SAMPLE:")
print(auth_df.head())
print()
print("AUTH shape:", auth_df.shape)
print("AUTH range:", auth_df["timestamp"].min(), "->", auth_df["timestamp"].max())

print("\n" + "=" * 50 + "\n")

print("REDTEAM:")
print(redteam_df.head())
print()
print("REDTEAM shape:", redteam_df.shape)
print("REDTEAM range:", redteam_df["timestamp"].min(), "->", redteam_df["timestamp"].max())