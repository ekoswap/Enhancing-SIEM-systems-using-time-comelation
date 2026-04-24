from lanl_parser import load_auth_sample, load_redteam
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events
from lanl_deduplicate import deduplicate_auth_events
from lanl_temporal_aggregate import temporal_aggregate_auth_events_relaxed

# -------------------------
# Load data
# -------------------------
auth_df = load_auth_sample("data/raw/auth_sample_late.txt")
auth_df = normalize_auth_df(auth_df)

redteam_df = load_redteam("data/raw/redteam.txt")

# -------------------------
# Build redteam reference sets
# -------------------------
redteam_users = set(redteam_df["user"].astype(str).unique())
redteam_src_computers = set(redteam_df["src_computer"].astype(str).unique())
redteam_dst_computers = set(redteam_df["dst_computer"].astype(str).unique())
redteam_all_computers = redteam_src_computers | redteam_dst_computers

# -------------------------
# Define redteam-related auth rows
# Rule:
# an auth row is considered related if:
# - src_user appears in redteam users
# OR
# - src_computer or dst_computer appears in redteam computers
# -------------------------
def mark_redteam_related_rows(df):
    working_df = df.copy()

    working_df["redteam_user_match"] = working_df["src_user"].astype(str).isin(redteam_users)
    working_df["redteam_src_computer_match"] = working_df["src_computer"].astype(str).isin(redteam_all_computers)
    working_df["redteam_dst_computer_match"] = working_df["dst_computer"].astype(str).isin(redteam_all_computers)

    working_df["redteam_related"] = (
        working_df["redteam_user_match"]
        | working_df["redteam_src_computer_match"]
        | working_df["redteam_dst_computer_match"]
    )

    return working_df

# -------------------------
# Stage 1: normalized
# -------------------------
normalized_df = mark_redteam_related_rows(auth_df)

# -------------------------
# Stage 2: filtered
# -------------------------
filtered_df = filter_auth_events(auth_df)
filtered_df = mark_redteam_related_rows(filtered_df)

# -------------------------
# Stage 3: deduplicated
# -------------------------
dedup_df = deduplicate_auth_events(filtered_df)
dedup_df = mark_redteam_related_rows(dedup_df)

# -------------------------
# Stage 4: aggregated
# For aggregated rows, define redteam_related if:
# - src_user matches a redteam user
# OR
# - src_computer or dst_computer matches a redteam computer
# -------------------------
aggregated_df = temporal_aggregate_auth_events_relaxed(dedup_df, time_window=5).copy()

aggregated_df["redteam_user_match"] = aggregated_df["src_user"].astype(str).isin(redteam_users)
aggregated_df["redteam_src_computer_match"] = aggregated_df["src_computer"].astype(str).isin(redteam_all_computers)
aggregated_df["redteam_dst_computer_match"] = aggregated_df["dst_computer"].astype(str).isin(redteam_all_computers)

aggregated_df["redteam_related"] = (
    aggregated_df["redteam_user_match"]
    | aggregated_df["redteam_src_computer_match"]
    | aggregated_df["redteam_dst_computer_match"]
)

# -------------------------
# Report
# -------------------------
def report_stage(name, df):
    total = len(df)
    related = int(df["redteam_related"].sum())
    pct = (related / total * 100) if total else 0

    print("=" * 60)
    print(name)
    print("=" * 60)
    print("Total rows:", total)
    print("Redteam-related rows:", related)
    print(f"Redteam-related %: {pct:.2f}%")
    print()

report_stage("NORMALIZED", normalized_df)
report_stage("FILTERED", filtered_df)
report_stage("DEDUPLICATED", dedup_df)
report_stage("AGGREGATED", aggregated_df)