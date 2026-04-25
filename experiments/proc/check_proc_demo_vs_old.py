from pathlib import Path
import pandas as pd

from pipeline.common import load_csv, mark_redteam_related, summarize_redteam_ratio
from pipeline.proc_strategy import PROC_COLUMNS, proc_filter_end_only, proc_best_aggregation

redteam_path = Path("data/raw/redteam.txt")
proc_sample_path = Path("data/raw/proc_redteam_sample.txt")

redteam_df = pd.read_csv(
    redteam_path,
    header=None,
    names=["timestamp", "user", "src_computer", "dst_computer"],
)

redteam_src = set(redteam_df["src_computer"].astype(str).unique())
redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())

df = load_csv(proc_sample_path, PROC_COLUMNS)

baseline = mark_redteam_related(
    df,
    "computer",
    "computer",
    redteam_src,
    redteam_dst,
)

filtered = proc_filter_end_only(baseline)
aggregated = proc_best_aggregation(filtered)

print("PIPELINE PROC CHECK")
print("=" * 50)
print("Baseline:", summarize_redteam_ratio(baseline))
print("Filtered:", summarize_redteam_ratio(filtered))
print("Aggregated:", summarize_redteam_ratio(aggregated))

print("\nExtra checks")
print("=" * 50)
print("Sample path:", proc_sample_path)
print("Rows in sample:", len(df))
print("Unique computers in sample:", df["computer"].nunique())
print("Event states:")
print(df["event_state"].value_counts())