from pathlib import Path

from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events
from lanl_deduplicate import deduplicate_auth_events
from lanl_temporal_aggregate import temporal_aggregate_auth_events_relaxed
from lanl_baseline_correlation import (
    baseline_correlation_raw,
    baseline_correlation_aggregated,
)

# ---------- Paths ----------
RAW_AUTH_PATH = Path("data/raw/auth_sample_late.txt")
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

NORMALIZED_OUT = OUTPUT_DIR / "normalized_auth_sample.csv"
FILTERED_OUT = OUTPUT_DIR / "filtered_auth_sample.csv"
DEDUP_OUT = OUTPUT_DIR / "deduplicated_auth_sample.csv"
AGGREGATED_OUT = OUTPUT_DIR / "aggregated_auth_sample.csv"
BEFORE_CORR_OUT = OUTPUT_DIR / "before_correlation.csv"
AFTER_CORR_OUT = OUTPUT_DIR / "after_correlation.csv"
SUMMARY_OUT = OUTPUT_DIR / "pipeline_summary.txt"

# ---------- Parameters ----------
TIME_WINDOW = 5


def main():
    # 1) Parse
    auth_df = load_auth_sample(RAW_AUTH_PATH)

    # 2) Normalize
    normalized_df = normalize_auth_df(auth_df)
    normalized_df.to_csv(NORMALIZED_OUT, index=False)

    # 3) Filter
    filtered_df = filter_auth_events(normalized_df)
    filtered_df.to_csv(FILTERED_OUT, index=False)

    # 4) Deduplicate
    deduplicated_df = deduplicate_auth_events(filtered_df)
    deduplicated_df.to_csv(DEDUP_OUT, index=False)

    # 5) Relaxed temporal aggregation
    aggregated_df = temporal_aggregate_auth_events_relaxed(
        deduplicated_df,
        time_window=TIME_WINDOW
    )
    aggregated_df.to_csv(AGGREGATED_OUT, index=False)

    # 6) Baseline correlation before / after
    before_corr_df = baseline_correlation_raw(
        deduplicated_df,
        time_window=TIME_WINDOW
    )
    before_corr_df.to_csv(BEFORE_CORR_OUT, index=False)

    after_corr_df = baseline_correlation_aggregated(
        aggregated_df,
        time_window=TIME_WINDOW
    )
    after_corr_df.to_csv(AFTER_CORR_OUT, index=False)

    # 7) Summary
    before_events = len(deduplicated_df)
    after_events = len(aggregated_df)
    event_reduction = before_events - after_events
    event_reduction_pct = (event_reduction / before_events * 100) if before_events else 0

    before_corr = len(before_corr_df)
    after_corr = len(after_corr_df)
    corr_change = after_corr - before_corr
    corr_change_pct = (corr_change / before_corr * 100) if before_corr else 0

    summary_lines = [
        "LANL Pre-Correlation Pipeline Summary",
        "=" * 40,
        "",
        f"Input auth events: {len(auth_df)}",
        f"After normalization: {len(normalized_df)}",
        f"After filtering: {len(filtered_df)}",
        f"After exact deduplication: {len(deduplicated_df)}",
        f"After relaxed temporal aggregation: {len(aggregated_df)}",
        "",
        f"Event reduction before vs after aggregation: {event_reduction} ({event_reduction_pct:.2f}%)",
        "",
        f"Baseline correlation results BEFORE aggregation: {before_corr}",
        f"Baseline correlation results AFTER aggregation: {after_corr}",
        f"Change in correlation result count: {corr_change} ({corr_change_pct:.2f}%)",
        "",
        f"Chosen strategy: relaxed aggregation",
        f"Chosen time window: {TIME_WINDOW}",
    ]

    summary_text = "\n".join(summary_lines)
    SUMMARY_OUT.write_text(summary_text, encoding="utf-8")

    print(summary_text)
    print("\nSaved files:")
    print("-", NORMALIZED_OUT)
    print("-", FILTERED_OUT)
    print("-", DEDUP_OUT)
    print("-", AGGREGATED_OUT)
    print("-", BEFORE_CORR_OUT)
    print("-", AFTER_CORR_OUT)
    print("-", SUMMARY_OUT)


if __name__ == "__main__":
    main()