from __future__ import annotations

from pathlib import Path
import pandas as pd

from pipeline.common import (
    load_csv,
    mark_redteam_related,
    mark_redteam_related_multi,
    summarize_redteam_ratio,
)
from pipeline.source_registry import SOURCE_REGISTRY


REDTEAM_PATH = Path("data/raw/redteam.txt")


def load_redteam_context():
    redteam_df = pd.read_csv(
        REDTEAM_PATH,
        header=None,
        names=["timestamp", "user", "src_computer", "dst_computer"],
    )

    redteam_src = set(redteam_df["src_computer"].astype(str).unique())
    redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())
    redteam_users = set(redteam_df["user"].astype(str).unique())

    return redteam_src, redteam_dst, redteam_users


def apply_redteam_marking(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    redteam_src, redteam_dst, redteam_users = load_redteam_context()

    if config["match_mode"] == "pair":
        return mark_redteam_related(
            df,
            config["src_col"],
            config["dst_col"],
            redteam_src,
            redteam_dst,
        )

    if config["match_mode"] == "multi":
        combined_values = redteam_src | redteam_dst | redteam_users
        return mark_redteam_related_multi(
            df,
            config["match_columns"],
            combined_values,
        )

    raise ValueError(f"Unsupported match_mode: {config['match_mode']}")

def run_source(source_name: str, data_path: str | Path):
    config = SOURCE_REGISTRY[source_name]

    print(f"\n=== {source_name.upper()} ===")

    if config.get("requires_preparation", False):
        print("Skipped in generic runner: this source requires a preparation/normalization stage before generic pipeline steps.")
        return

    df = load_csv(data_path, config["columns"])
    baseline = apply_redteam_marking(df, config)

    print("Baseline:", summarize_redteam_ratio(baseline))

    current = baseline

    if config["filter_fn"] is not None:
        current = config["filter_fn"](current)
        print("Filtered:", summarize_redteam_ratio(current))

    if config["dedup_fn"] is not None:
        current = config["dedup_fn"](current)
        print("Deduplicated:", summarize_redteam_ratio(current))

    if config["aggregate_fn"] is not None:
        current = config["aggregate_fn"](current)
        print("Aggregated:", summarize_redteam_ratio(current))

if __name__ == "__main__":
    run_source("dns", "data/raw/dns_redteam_sample.txt")
    run_source("flows", "data/raw/flows_redteam_sample.txt")
    run_source("proc", "data/raw/proc_redteam_sample.txt")
    run_source("auth", "data/raw/auth_sample_late.txt")