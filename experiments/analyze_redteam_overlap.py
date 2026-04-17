from lanl_parser import load_auth_sample, load_redteam
from lanl_normalizer import normalize_auth_df
import pandas as pd

auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
auth_df = normalize_auth_df(auth_df)

redteam_df = load_redteam("data/raw/redteam.txt")

print("=" * 60)
print("TIME OVERLAP")
print("=" * 60)
print("AUTH range:", auth_df["timestamp"].min(), "->", auth_df["timestamp"].max())
print("REDTEAM range:", redteam_df["timestamp"].min(), "->", redteam_df["timestamp"].max())

time_overlap_df = auth_df[
    (auth_df["timestamp"] >= redteam_df["timestamp"].min()) &
    (auth_df["timestamp"] <= redteam_df["timestamp"].max())
]

print("\nAuth rows inside redteam time range:", len(time_overlap_df))

print("\n" + "=" * 60)
print("USER OVERLAP")
print("=" * 60)

auth_users = set(auth_df["src_user"].astype(str).unique())
redteam_users = set(redteam_df["user"].astype(str).unique())

common_users = sorted(auth_users & redteam_users)

print("Unique auth src_user count:", len(auth_users))
print("Unique redteam user count:", len(redteam_users))
print("Common users:", len(common_users))
print("First common users:", common_users[:20])

print("\n" + "=" * 60)
print("COMPUTER OVERLAP")
print("=" * 60)

auth_src_computers = set(auth_df["src_computer"].astype(str).unique())
auth_dst_computers = set(auth_df["dst_computer"].astype(str).unique())
auth_all_computers = auth_src_computers | auth_dst_computers

red_src_computers = set(redteam_df["src_computer"].astype(str).unique())
red_dst_computers = set(redteam_df["dst_computer"].astype(str).unique())
red_all_computers = red_src_computers | red_dst_computers

common_computers = sorted(auth_all_computers & red_all_computers)

print("Unique auth computers:", len(auth_all_computers))
print("Unique redteam computers:", len(red_all_computers))
print("Common computers:", len(common_computers))
print("First common computers:", common_computers[:30])

print("\n" + "=" * 60)
print("ROW-LEVEL MATCH CANDIDATES")
print("=" * 60)

candidate_df = auth_df.merge(
    redteam_df,
    left_on=["src_user", "src_computer", "dst_computer"],
    right_on=["user", "src_computer", "dst_computer"],
    how="inner",
    suffixes=("_auth", "_red")
)

if not candidate_df.empty:
    candidate_df["time_diff"] = candidate_df["timestamp_auth"] - candidate_df["timestamp_red"]

print("Candidate row matches:", len(candidate_df))

if not candidate_df.empty:
    print("\nFirst 10 candidate matches:")
    print(candidate_df[
        ["timestamp_auth", "timestamp_red", "time_diff", "src_user", "src_computer", "dst_computer"]
    ].head(10))
else:
    print("No direct row-level matches found.")