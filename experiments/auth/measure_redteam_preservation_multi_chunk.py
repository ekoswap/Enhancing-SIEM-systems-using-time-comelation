from pathlib import Path
import pandas as pd

from lanl_parser import load_auth_sample, load_redteam
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events
from lanl_deduplicate import deduplicate_auth_events
from lanl_temporal_aggregate import temporal_aggregate_auth_events_relaxed

files = [
    "auth_sample_late.txt",
    "auth_sample_chunk2.txt",
    "auth_sample_chunk3.txt",
]

redteam_df = load_redteam("data/raw/redteam.txt")

redteam_users = set(redteam_df["user"].astype(str).unique())
redteam_src_computers = set(redteam_df["src_computer"].astype(str).unique())
redteam_dst_computers = set(redteam_df["dst_computer"].astype(str).unique())
redteam_all_computers = redteam_src_computers | redteam_dst_computers


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


results = []

for file_name in files:
    auth_df = load_auth_sample(Path("data/raw") / file_name)
    auth_df = normalize_auth_df(auth_df)

    normalized_df = mark_redteam_related_rows(auth_df)

    filtered_df = filter_auth_events(auth_df)
    filtered_df = mark_redteam_related_rows(filtered_df)

    dedup_df = deduplicate_auth_events(filtered_df)
    dedup_df = mark_redteam_related_rows(dedup_df)

    aggregated_df = temporal_aggregate_auth_events_relaxed(dedup_df, time_window=5).copy()

    aggregated_df["redteam_user_match"] = aggregated_df["src_user"].astype(str).isin(redteam_users)
    aggregated_df["redteam_src_computer_match"] = aggregated_df["src_computer"].astype(str).isin(redteam_all_computers)
    aggregated_df["redteam_dst_computer_match"] = aggregated_df["dst_computer"].astype(str).isin(redteam_all_computers)

    aggregated_df["redteam_related"] = (
        aggregated_df["redteam_user_match"]
        | aggregated_df["redteam_src_computer_match"]
        | aggregated_df["redteam_dst_computer_match"]
    )

    normalized_total = len(normalized_df)
    normalized_related = int(normalized_df["redteam_related"].sum())
    normalized_pct = (normalized_related / normalized_total * 100) if normalized_total else 0

    aggregated_total = len(aggregated_df)
    aggregated_related = int(aggregated_df["redteam_related"].sum())
    aggregated_pct = (aggregated_related / aggregated_total * 100) if aggregated_total else 0

    pct_change = aggregated_pct - normalized_pct

    results.append({
        "file_name": file_name,
        "normalized_total": normalized_total,
        "normalized_redteam_related": normalized_related,
        "normalized_redteam_pct": round(normalized_pct, 2),
        "aggregated_total": aggregated_total,
        "aggregated_redteam_related": aggregated_related,
        "aggregated_redteam_pct": round(aggregated_pct, 2),
        "redteam_pct_change": round(pct_change, 2),
    })

results_df = pd.DataFrame(results)
print(results_df)

output_path = Path("outputs/redteam_preservation_multi_chunk.csv")
results_df.to_csv(output_path, index=False)

print("\nSaved to:", output_path)