from pathlib import Path
import pandas as pd

output_dir = Path("outputs")
output_dir.mkdir(exist_ok=True)

rows = [
    {
        "source": "auth",
        "role": "main prototype path",
        "status": "adopted",
        "key_strategy": "improved filtering + relaxed temporal aggregation",
        "strength": "strongest",
        "reason": "Provided the clearest end-to-end pre-correlation pipeline, repeated reduction in correlation outputs, and stable preservation of redteam-related context."
    },
    {
        "source": "proc",
        "role": "exploratory extension",
        "status": "strong exploratory extension",
        "key_strategy": "End-only filtering",
        "strength": "high",
        "reason": "Showed repeated gains in redteam-related concentration across multiple chunks, but remained secondary to auth."
    },
    {
        "source": "flows",
        "role": "exploratory extension",
        "status": "strong exploratory extension",
        "key_strategy": "duration > 0 + aggregation by destination_computer and protocol",
        "strength": "high",
        "reason": "Showed repeated gains in redteam-related concentration across eight chunks with a generalizable filtering rule, but remained less mature than auth."
    },
    {
    "source": "dns",
    "role": "exploratory extension",
    "status": "exploratory extension",
    "key_strategy": "aggregation by source_computer + destination_computer",
    "strength": "moderate",
    "reason": "Showed high redteam-related concentration across multiple chunks and remained useful after source-destination aggregation, though still less mature than auth, proc, and flows."
    },
]

df = pd.DataFrame(rows)

csv_path = output_dir / "data_source_comparison.csv"
txt_path = output_dir / "data_source_comparison_summary.txt"

df.to_csv(csv_path, index=False)

lines = [
    "Data Source Comparison Summary",
    "=" * 40,
    "",
]

for _, row in df.iterrows():
    lines.extend([
        f"Source: {row['source']}",
        f"- Role: {row['role']}",
        f"- Status: {row['status']}",
        f"- Key strategy: {row['key_strategy']}",
        f"- Strength: {row['strength']}",
        f"- Reason: {row['reason']}",
        "",
    ])

text = "\n".join(lines)
txt_path.write_text(text, encoding="utf-8")

print(df)
print("\nSaved files:")
print("-", csv_path)
print("-", txt_path)