from pathlib import Path
import pandas as pd

input_path = Path("outputs/compare_proc_strategies.csv")
output_path = Path("outputs/proc_exploratory_summary.txt")

df = pd.read_csv(input_path)

best_row = df.loc[df["redteam_related_pct"].idxmax()]

lines = [
    "PROC Exploratory Summary",
    "=" * 40,
    "",
    "Exploration goal:",
    "Evaluate simple process-event strategies to see whether proc data can preserve redteam-related context in a useful way.",
    "",
    "Tested strategies:",
]

for _, row in df.iterrows():
    lines.append(
        f"- {row['strategy']}: total_rows={row['total_rows']}, "
        f"redteam_related_rows={row['redteam_related_rows']}, "
        f"redteam_related_pct={row['redteam_related_pct']:.2f}%"
    )

lines.extend([
    "",
    "Best observed strategy:",
    f"- Strategy: {best_row['strategy']}",
    f"- Total rows: {best_row['total_rows']}",
    f"- Redteam-related rows: {best_row['redteam_related_rows']}",
    f"- Redteam-related percentage: {best_row['redteam_related_pct']:.2f}%",
    "",
    "Interpretation:",
    "The End-only strategy preserved the highest proportion of redteam-related context in the explored proc sample.",
    "This suggests that process data may support the project as a secondary exploratory extension when handled with an event-state-aware strategy.",
    "",
    "Decision:",
    "PROC is not promoted to the main prototype pipeline, but it is considered a promising exploratory extension.",
])

text = "\n".join(lines)
output_path.write_text(text, encoding="utf-8")

print(text)
print("\nSaved to:", output_path)