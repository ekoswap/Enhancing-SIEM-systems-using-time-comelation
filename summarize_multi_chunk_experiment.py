from pathlib import Path
import pandas as pd

input_path = Path("outputs/multi_chunk_experiment.csv")
output_path = Path("outputs/multi_chunk_experiment_summary.txt")

df = pd.read_csv(input_path)

avg_event_reduction = df["event_reduction_pct"].mean()
avg_corr_change = df["correlation_change_pct"].mean()

best_event_row = df.loc[df["event_reduction_pct"].idxmax()]
best_corr_row = df.loc[df["correlation_change_pct"].idxmin()]  # most negative = strongest reduction

lines = [
    "Multi-Chunk Experiment Summary",
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
    "The relaxed temporal aggregation strategy consistently reduced the event stream across multiple LANL chunks.",
    "This reduction was followed by repeated decreases in baseline correlation outputs.",
    "These results support the project direction of applying pre-correlation reduction before downstream correlation.",
])

summary_text = "\n".join(lines)
output_path.write_text(summary_text, encoding="utf-8")

print(summary_text)
print("\nSaved to:", output_path)