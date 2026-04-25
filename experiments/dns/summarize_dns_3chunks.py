from pathlib import Path
import pandas as pd

input_path = Path("outputs/dns_best_strategy_3chunks.csv")
output_path = Path("outputs/dns_3chunks_summary.txt")

df = pd.read_csv(input_path)

avg_baseline = df["baseline_redteam_pct"].mean()
avg_agg = df["agg_redteam_pct"].mean()

best_agg_row = df.loc[df["agg_redteam_pct"].idxmax()]

lines = [
    "DNS 3-Chunks Summary",
    "=" * 40,
    "",
    "Exploration goal:",
    "Evaluate whether DNS communication aggregation can preserve or improve the concentration of redteam-related context across multiple chunks.",
    "",
    "Selected strategy:",
    "- Aggregation key: source_computer + destination_computer",
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
        f"  Aggregated total: {row['agg_total']}",
        f"  Aggregated redteam-related %: {row['agg_redteam_pct']:.2f}%",
        "",
    ])

lines.extend([
    "Overall averages:",
    f"- Average baseline redteam-related %: {avg_baseline:.2f}%",
    f"- Average aggregated redteam-related %: {avg_agg:.2f}%",
    "",
    "Best observed value:",
    f"- Best aggregated chunk: {best_agg_row['file_name']} ({best_agg_row['agg_redteam_pct']:.2f}%)",
    "",
    "Interpretation:",
    "DNS aggregation by source and destination computer preserved a high concentration of redteam-related context across the tested chunks.",
    "This suggests that DNS can serve as an exploratory extension, although its role remains less central than the auth-based main path.",
    "",
    "Decision:",
    "DNS is treated as an exploratory extension in the current prototype stage.",
])

text = "\n".join(lines)
output_path.write_text(text, encoding="utf-8")

print(text)
print("\nSaved to:", output_path)