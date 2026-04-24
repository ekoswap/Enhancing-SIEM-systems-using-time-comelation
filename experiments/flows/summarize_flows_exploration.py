from pathlib import Path
import pandas as pd

input_path = Path("outputs/flows_duration_only_best_agg_3chunks.csv")
output_path = Path("outputs/flows_exploratory_summary.txt")

df = pd.read_csv(input_path)

avg_baseline = df["baseline_redteam_pct"].mean()
avg_duration = df["duration_only_redteam_pct"].mean()
avg_agg = df["agg_redteam_pct"].mean()

best_duration_row = df.loc[df["duration_only_redteam_pct"].idxmax()]
best_agg_row = df.loc[df["agg_redteam_pct"].idxmax()]

lines = [
    "FLOWS Exploratory Summary",
    "=" * 40,
    "",
    "Exploration goal:",
    "Evaluate whether a generalizable preprocessing strategy can improve the concentration of redteam-related context in network-flow data.",
    "",
    "Selected strategy:",
    "- Filtering: duration > 0",
    "- Aggregation key: destination_computer + protocol",
    "",
    f"Number of tested chunks: {len(df)}",
    "",
    "Per-chunk results:",
]

for _, row in df.iterrows():
    lines.extend([
        f"- {row['file_name']}",
        f"  Baseline total: {row['baseline_total']}",
        f"  Baseline redteam-related %: {row['baseline_redteam_pct']:.2f}%",
        f"  Duration-only total: {row['duration_only_total']}",
        f"  Duration-only redteam-related %: {row['duration_only_redteam_pct']:.2f}%",
        f"  Aggregated total: {row['agg_total']}",
        f"  Aggregated redteam-related %: {row['agg_redteam_pct']:.2f}%",
        "",
    ])

lines.extend([
    "Overall averages:",
    f"- Average baseline redteam-related %: {avg_baseline:.2f}%",
    f"- Average duration-only redteam-related %: {avg_duration:.2f}%",
    f"- Average aggregated redteam-related %: {avg_agg:.2f}%",
    "",
    "Best observed values:",
    f"- Best duration-only chunk: {best_duration_row['file_name']} ({best_duration_row['duration_only_redteam_pct']:.2f}%)",
    f"- Best aggregated chunk: {best_agg_row['file_name']} ({best_agg_row['agg_redteam_pct']:.2f}%)",
    "",
    "Interpretation:",
    "A duration-based filtering rule improved the concentration of redteam-related context across the tested flows chunks without relying on a protocol-specific assumption.",
    "Aggregation by destination_computer and protocol remained useful in some chunks, but showed less stability than the proc extension.",
    "",
    "Decision:",
    "FLOWS is treated as an exploratory extension. It is more promising than DNS, but less mature than AUTH and PROC in the current prototype stage.",
])

text = "\n".join(lines)
output_path.write_text(text, encoding="utf-8")

print(text)
print("\nSaved to:", output_path)
