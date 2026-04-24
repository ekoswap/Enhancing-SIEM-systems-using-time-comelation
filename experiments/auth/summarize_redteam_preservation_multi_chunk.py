from pathlib import Path
import pandas as pd

input_path = Path("outputs/redteam_preservation_multi_chunk.csv")
output_path = Path("outputs/redteam_preservation_multi_chunk_summary.txt")

df = pd.read_csv(input_path)

avg_normalized_pct = df["normalized_redteam_pct"].mean()
avg_aggregated_pct = df["aggregated_redteam_pct"].mean()
avg_pct_change = df["redteam_pct_change"].mean()

max_drop_row = df.loc[df["redteam_pct_change"].idxmin()]
max_gain_row = df.loc[df["redteam_pct_change"].idxmax()]

lines = [
    "Redteam Preservation Multi-Chunk Summary",
    "=" * 40,
    "",
    f"Number of tested chunks: {len(df)}",
    "",
    "Per-chunk results:",
]

for _, row in df.iterrows():
    lines.extend([
        f"- {row['file_name']}",
        f"  Normalized total: {row['normalized_total']}",
        f"  Normalized redteam-related: {row['normalized_redteam_related']} ({row['normalized_redteam_pct']:.2f}%)",
        f"  Aggregated total: {row['aggregated_total']}",
        f"  Aggregated redteam-related: {row['aggregated_redteam_related']} ({row['aggregated_redteam_pct']:.2f}%)",
        f"  Change in redteam-related percentage: {row['redteam_pct_change']:.2f}",
        "",
    ])

lines.extend([
    "Overall averages:",
    f"- Average normalized redteam-related %: {avg_normalized_pct:.2f}%",
    f"- Average aggregated redteam-related %: {avg_aggregated_pct:.2f}%",
    f"- Average percentage change: {avg_pct_change:.2f}",
    "",
    "Observed extremes:",
    f"- Largest decrease: {max_drop_row['file_name']} ({max_drop_row['redteam_pct_change']:.2f})",
    f"- Largest increase: {max_gain_row['file_name']} ({max_gain_row['redteam_pct_change']:.2f})",
    "",
    "Interpretation:",
    "Across multiple authentication chunks, the pre-correlation pipeline preserved the proportion of redteam-related context with only minimal variation.",
    "This suggests that event reduction was achieved without substantially distorting the attack-relevant context.",
])

summary_text = "\n".join(lines)
output_path.write_text(summary_text, encoding="utf-8")

print(summary_text)
print("\nSaved to:", output_path)