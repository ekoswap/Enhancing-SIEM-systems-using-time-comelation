from pathlib import Path
import pandas as pd

pd.set_option("display.max_columns", None)

from lanl_parser import load_auth_sample
from lanl_normalizer import normalize_auth_df
from lanl_filter import filter_auth_events
from lanl_deduplicate import deduplicate_auth_events
from lanl_temporal_aggregate import temporal_aggregate_auth_events_relaxed
from lanl_baseline_correlation import (
    baseline_correlation_raw,
    baseline_correlation_aggregated,
)

files = [
    "auth_redteam_zone_chunk_1.txt",
    "auth_redteam_zone_chunk_2.txt",
    "auth_redteam_zone_chunk_3.txt",
    "auth_redteam_zone_chunk_4.txt",
    "auth_redteam_zone_chunk_5.txt",
    "auth_redteam_zone_chunk_6.txt",
    "auth_redteam_zone_chunk_7.txt",
    "auth_redteam_zone_chunk_8.txt",
]

TIME_WINDOW = 5
results = []

for file_name in files:
    file_path = Path("data/raw") / file_name

    auth_df = load_auth_sample(file_path)
    normalized_df = normalize_auth_df(auth_df)
    filtered_df = filter_auth_events(normalized_df)
    deduplicated_df = deduplicate_auth_events(filtered_df)
    aggregated_df = temporal_aggregate_auth_events_relaxed(
        deduplicated_df,
        time_window=TIME_WINDOW
    )

    before_corr_df = baseline_correlation_raw(
        deduplicated_df,
        time_window=TIME_WINDOW
    )
    after_corr_df = baseline_correlation_aggregated(
        aggregated_df,
        time_window=TIME_WINDOW
    )

    before_events = len(deduplicated_df)
    after_events = len(aggregated_df)
    event_reduction = before_events - after_events
    event_reduction_pct = (event_reduction / before_events * 100) if before_events else 0

    before_corr = len(before_corr_df)
    after_corr = len(after_corr_df)
    corr_change = after_corr - before_corr
    corr_change_pct = (corr_change / before_corr * 100) if before_corr else 0

    results.append({
        "file_name": file_name,
        "input_events": len(auth_df),
        "after_filter": len(filtered_df),
        "after_dedup": len(deduplicated_df),
        "after_aggregation": len(aggregated_df),
        "event_reduction": event_reduction,
        "event_reduction_pct": round(event_reduction_pct, 2),
        "before_correlation": before_corr,
        "after_correlation": after_corr,
        "correlation_change": corr_change,
        "correlation_change_pct": round(corr_change_pct, 2) if before_corr else None,
    })

results_df = pd.DataFrame(results)
print(results_df)

output_path = Path("outputs/redteam_zone_experiment.csv")
results_df.to_csv(output_path, index=False)

print("\nSaved to:", output_path)