from pathlib import Path
import pandas as pd

from pipeline.common import load_csv, summarize_redteam_ratio, mark_redteam_related_multi
from pipeline.proc_strategy import PROC_COLUMNS, proc_filter_end_only, proc_best_aggregation

redteam_path = Path("data/raw/redteam.txt")
proc_sample_path = Path("data/raw/proc_redteam_sample.txt")

redteam_df = pd.read_csv(
    redteam_path,
    header=None,
    names=["timestamp", "user", "src_computer", "dst_computer"],
)

redteam_users = set(redteam_df["user"].astype(str).unique())
redteam_computers = set(redteam_df["src_computer"].astype(str).unique()) | set(redteam_df["dst_computer"].astype(str).unique())
combined_values = redteam_users | redteam_computers

df = load_csv(proc_sample_path, PROC_COLUMNS)

baseline = mark_redteam_related_multi(
    df,
    ["user", "computer"],
    combined_values,
)

filtered = proc_filter_end_only(baseline)
aggregated = proc_best_aggregation(filtered)

print("PROC USER+COMPUTER CHECK")
print("=" * 50)
print("Baseline:", summarize_redteam_ratio(baseline))
print("Filtered:", summarize_redteam_ratio(filtered))
print("Aggregated:", summarize_redteam_ratio(aggregated))