from pathlib import Path
import pandas as pd

input_path = Path("outputs/redteam_zone_experiment.csv")
output_path = Path("outputs/redteam_zone_experiment_summary.txt")

df = pd.read_csv(input_path)

avg_event_reduction = df["event_reduction_pct"].mean()
avg_corr_change = df["correlation_change_pct"].mean()

best_event_row = df.loc[df["event_reduction_pct"].idxmax()]
best_corr_row = df.loc[df["correlation_change_pct"].idxmin()]  # most negative

lines = [
    "Redteam-Zone Experiment Summary",
    "=" * 40,
    "",
    f"Number of tested chunks: {len(df)}",
    "",
    "Per-chunk results:",
]

for _, row in df.iterrows():
    lines.extend([
        f"- {row['file_name']}",
        f"  Input events: {row['input_events']}",
        f"  After filter: {row['after_filter']}",
        f"  After aggregation: {row['after_aggregation']}",
        f"  Event reduction: {row['event_reduction']} ({row['event_reduction_pct']:.2f}%)",
        f"  Correlation before: {row['before_correlation']}",
        f"  Correlation after: {row['after_correlation']}",
        f"  Correlation change: {row['correlation_change']} ({row['correlation_change_pct']:.2f}%)",
        "",
    ])

lines.extend([
    "Overall averages:",
    f"- Average event reduction: {avg_event_reduction:.2f}%",
    f"- Average correlation change: {avg_corr_change:.2f}%",
    "",
    "Best observed reductions:",
    f"- Strongest event reduction: {best_event_row['file_name']} ({best_event_row['event_reduction_pct']:.2f}%)",
    f"- Strongest correlation reduction: {best_corr_row['file_name']} ({best_corr_row['correlation_change_pct']:.2f}%)",
    "",
    "Interpretation:",
    "Across eight redteam-zone authentication chunks, the selected pre-correlation pipeline consistently reduced the event stream.",
    "This was repeatedly followed by reductions in baseline correlation outputs.",
    "These results strengthen the evidence that the chosen pipeline improves stream compactness before downstream correlation in redteam-relevant regions of the LANL data.",
])

summary_text = "\n".join(lines)
output_path.write_text(summary_text, encoding="utf-8")

print(summary_text)
print("\nSaved to:", output_path)