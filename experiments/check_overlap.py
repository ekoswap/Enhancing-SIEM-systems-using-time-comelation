from pathlib import Path
import pandas as pd

auth_path = Path("data/raw/auth_sample_late.txt")
redteam_path = Path("data/raw/redteam.txt")

auth_df = pd.read_csv(
    auth_path,
    header=None,
    names=[
        "timestamp",
        "src_user",
        "dst_user",
        "src_computer",
        "dst_computer",
        "auth_type",
        "logon_type",
        "auth_orientation",
        "result"
    ]
)

redteam_df = pd.read_csv(
    redteam_path,
    header=None,
    names=[
        "timestamp",
        "user",
        "src_computer",
        "dst_computer"
    ]
)

print("AUTH RANGE:", auth_df["timestamp"].min(), "->", auth_df["timestamp"].max())
print("REDTEAM RANGE:", redteam_df["timestamp"].min(), "->", redteam_df["timestamp"].max())

overlap_df = auth_df[
    (auth_df["timestamp"] >= redteam_df["timestamp"].min()) &
    (auth_df["timestamp"] <= redteam_df["timestamp"].max())
]

print("\nRows from auth_sample_late inside redteam time range:", len(overlap_df))

print("\nFirst 10 matching rows:")
print(overlap_df.head(10))