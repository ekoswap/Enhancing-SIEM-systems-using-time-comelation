from __future__ import annotations

from pathlib import Path
import pandas as pd

from pipeline.common import load_csv, mark_redteam_related, summarize_redteam_ratio
from pipeline.dns_strategy import DNS_COLUMNS, dns_best_aggregation
from pipeline.flows_strategy import FLOWS_COLUMNS, flows_filter_duration_only, flows_best_aggregation
from pipeline.proc_strategy import PROC_COLUMNS, proc_filter_end_only, proc_best_aggregation


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


def run_dns_demo():
    path = Path("data/raw/dns_redteam_sample.txt")
    df = load_csv(path, DNS_COLUMNS)
    redteam_src, redteam_dst = load_redteam_sets()

    baseline = mark_redteam_related(df, "source_computer", "destination_computer", redteam_src, redteam_dst)
    aggregated = dns_best_aggregation(baseline)

    print("\n=== DNS DEMO ===")
    print("Baseline:", summarize_redteam_ratio(baseline))
    print("Aggregated:", summarize_redteam_ratio(aggregated))


def run_flows_demo():
    path = Path("data/raw/flows_redteam_sample.txt")
    df = load_csv(path, FLOWS_COLUMNS)
    redteam_src, redteam_dst = load_redteam_sets()

    baseline = mark_redteam_related(df, "source_computer", "destination_computer", redteam_src, redteam_dst)
    filtered = flows_filter_duration_only(baseline)
    aggregated = flows_best_aggregation(filtered)

    print("\n=== FLOWS DEMO ===")
    print("Baseline:", summarize_redteam_ratio(baseline))
    print("Filtered:", summarize_redteam_ratio(filtered))
    print("Aggregated:", summarize_redteam_ratio(aggregated))


def run_proc_demo():
    path = Path("data/raw/proc_redteam_sample.txt")
    df = load_csv(path, PROC_COLUMNS)
    redteam_src, redteam_dst = load_redteam_sets()

    baseline = mark_redteam_related(df, "computer", "computer", redteam_src, redteam_dst)
    filtered = proc_filter_end_only(baseline)
    aggregated = proc_best_aggregation(filtered)

    print("\n=== PROC DEMO ===")
    print("Baseline:", summarize_redteam_ratio(baseline))
    print("Filtered:", summarize_redteam_ratio(filtered))
    print("Aggregated:", summarize_redteam_ratio(aggregated))


if __name__ == "__main__":
    run_dns_demo()
    run_flows_demo()
    run_proc_demo()