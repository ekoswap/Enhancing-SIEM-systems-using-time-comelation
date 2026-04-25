from __future__ import annotations

from pathlib import Path
import pandas as pd

from pipeline.auth_preparation import prepare_auth_dataframe
from pipeline.auth_strategy import auth_filter_improved, auth_exact_dedup, auth_best_aggregation
from pipeline.common import mark_redteam_related, summarize_redteam_ratio


AUTH_PATH = Path("data/raw/auth_sample_late.txt")
REDTEAM_PATH = Path("data/raw/redteam.txt")


def load_redteam_sets() -> tuple[set[str], set[str]]:
    redteam_df = pd.read_csv(
        REDTEAM_PATH,
        header=None,
        names=["timestamp", "user", "src_computer", "dst_computer"],
    )
    redteam_src = set(redteam_df["src_computer"].astype(str).unique())
    redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())
    return redteam_src, redteam_dst


def run_auth_pipeline():
    df = prepare_auth_dataframe(AUTH_PATH)
    redteam_src, redteam_dst = load_redteam_sets()

    baseline = mark_redteam_related(
        df,
        "src_computer",
        "dst_computer",
        redteam_src,
        redteam_dst,
    )

    print("\n=== AUTH PREPARED ===")
    print("Baseline:", summarize_redteam_ratio(baseline))

    filtered = auth_filter_improved(baseline)
    print("Filtered:", summarize_redteam_ratio(filtered))

    deduped = auth_exact_dedup(filtered)
    print("Deduplicated:", summarize_redteam_ratio(deduped))

    aggregated = auth_best_aggregation(deduped)

    aggregated_marked = mark_redteam_related(
       aggregated,
       "src_computer",
       "dst_computer",
       redteam_src,
       redteam_dst,
)

    print("Aggregated:", summarize_redteam_ratio(aggregated_marked))

if __name__ == "__main__":
    run_auth_pipeline()