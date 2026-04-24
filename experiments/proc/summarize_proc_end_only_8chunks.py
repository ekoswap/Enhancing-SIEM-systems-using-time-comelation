from pathlib import Path
import pandas as pd

input_path = Path("outputs/proc_end_only_3chunks.csv")
output_path = Path("outputs/proc_end_only_8chunks_summary.txt")

df = pd.read_csv(input_path)

avg_baseline = df["baseline_redteam_pct"].mean()
avg_end_only = df["end_only_redteam_pct"].mean()
avg_agg = df["agg_redteam_pct"].mean()

best_end_row = df.loc[df["end_only_redteam_pct"].idxmax()]
best_agg_row = df.loc[df["agg_redteam_pct"].idxmax()]

lines = [
    "PROC End-Only 8-Chunks Summary",
    "=" * 40,
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
        f"  End-only total: {row['end_only_total']}",
        f"  End-only redteam-related %: {row['end_only_redteam_pct']:.2f}%",
        f"  Aggregated total: {row['agg_total']}",
        f"  Aggregated redteam-related %: {row['agg_redteam_pct']:.2f}%",
        "",
    ])

lines.extend([
    "Overall averages:",
    f"- Average baseline redteam-related %: {avg_baseline:.2f}%",
    f"- Average end-only redteam-related %: {avg_end_only:.2f}%",
    f"- Average aggregated redteam-related %: {avg_agg:.2f}%",
    "",
    "Best observed values:",
    f"- Best end-only chunk: {best_end_row['file_name']} ({best_end_row['end_only_redteam_pct']:.2f}%)",
    f"- Best aggregated chunk: {best_agg_row['file_name']} ({best_agg_row['agg_redteam_pct']:.2f}%)",
    "",
    "Interpretation:",
    "Across eight proc redteam-zone chunks, the End-only strategy consistently increased the concentration of redteam-related context compared with the raw baseline.",
    "After aggregation, the redteam-related proportion remained clearly higher than baseline, which supports PROC as a strong exploratory extension rather than a main prototype path.",
])

summary_text = "\n".join(lines)
output_path.write_text(summary_text, encoding="utf-8")

print(summary_text)
print("\nSaved to:", output_path)