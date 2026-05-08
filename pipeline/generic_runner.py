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
from pipeline.auth_preparation import prepare_auth_dataframe


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


def prepare_source_dataframe(source_name: str, data_path: str | Path):
    config = SOURCE_REGISTRY[source_name]

    if config.get("preparation_fn") == "prepare_auth_dataframe":
        return prepare_auth_dataframe(data_path)

    return load_csv(data_path, config["columns"])


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


def can_remark_after_aggregation(df: pd.DataFrame, config: dict) -> bool:
    if config["match_mode"] == "pair":
        return config["src_col"] in df.columns and config["dst_col"] in df.columns

    if config["match_mode"] == "multi":
        return all(col in df.columns for col in config["match_columns"])

    return False


def run_source(source_name: str, data_path: str | Path) -> dict:
    config = SOURCE_REGISTRY[source_name]

    df = prepare_source_dataframe(source_name, data_path)
    baseline = apply_redteam_marking(df, config)

    baseline_summary = summarize_redteam_ratio(baseline)

    result = {
        "source": source_name,
        "data_path": str(data_path),
        "baseline_total_rows": baseline_summary["total_rows"],
        "baseline_redteam_rows": baseline_summary["redteam_related_rows"],
        "baseline_redteam_pct": baseline_summary["redteam_related_pct"],
        "filtered_total_rows": None,
        "filtered_redteam_rows": None,
        "filtered_redteam_pct": None,
        "deduplicated_total_rows": None,
        "deduplicated_redteam_rows": None,
        "deduplicated_redteam_pct": None,
        "aggregated_total_rows": None,
        "aggregated_redteam_rows": None,
        "aggregated_redteam_pct": None,
    }

    print(f"\n=== {source_name.upper()} ===")
    print("Baseline:", baseline_summary)

    current = baseline

    if config["filter_fn"] is not None:
        current = config["filter_fn"](current)
        filtered_summary = summarize_redteam_ratio(current)
        print("Filtered:", filtered_summary)

        result["filtered_total_rows"] = filtered_summary["total_rows"]
        result["filtered_redteam_rows"] = filtered_summary["redteam_related_rows"]
        result["filtered_redteam_pct"] = filtered_summary["redteam_related_pct"]

    if config["dedup_fn"] is not None:
        current = config["dedup_fn"](current)
        dedup_summary = summarize_redteam_ratio(current)
        print("Deduplicated:", dedup_summary)

        result["deduplicated_total_rows"] = dedup_summary["total_rows"]
        result["deduplicated_redteam_rows"] = dedup_summary["redteam_related_rows"]
        result["deduplicated_redteam_pct"] = dedup_summary["redteam_related_pct"]

    if config["aggregate_fn"] is not None:
        current = config["aggregate_fn"](current)

        if can_remark_after_aggregation(current, config):
            current = apply_redteam_marking(current, config)

        aggregated_summary = summarize_redteam_ratio(current)
        print("Aggregated:", aggregated_summary)

        result["aggregated_total_rows"] = aggregated_summary["total_rows"]
        result["aggregated_redteam_rows"] = aggregated_summary["redteam_related_rows"]
        result["aggregated_redteam_pct"] = aggregated_summary["redteam_related_pct"]

    return result


def run_all_sources() -> list[dict]:
    source_inputs = [
        ("dns", "data/raw/dns_redteam_sample.txt"),
        ("flows", "data/raw/flows_redteam_sample.txt"),
        ("proc", "data/raw/proc_redteam_sample.txt"),
        ("auth", "data/raw/auth_sample_late.txt"),
    ]

    results = []
    for source_name, data_path in source_inputs:
        results.append(run_source(source_name, data_path))

    return results


if __name__ == "__main__":
    run_all_sources()