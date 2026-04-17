from pathlib import Path

output_dir = Path("outputs")

pipeline_summary_path = output_dir / "pipeline_summary.txt"
multi_chunk_summary_path = output_dir / "multi_chunk_experiment_summary.txt"
decision_summary_path = output_dir / "final_experiment_decision_summary.txt"
redteam_summary_path = output_dir / "redteam_overlap_summary.txt"

master_summary_path = output_dir / "master_project_summary.txt"

sections = [
    ("Pipeline Summary", pipeline_summary_path),
    ("Multi-Chunk Experiment Summary", multi_chunk_summary_path),
    ("Final Experiment Decision Summary", decision_summary_path),
    ("Redteam Overlap Summary", redteam_summary_path),
]

lines = [
    "Master Project Summary",
    "=" * 60,
    "",
    "This file combines the main experimental summaries generated during development.",
    "",
]

for title, path in sections:
    lines.append("=" * 60)
    lines.append(title)
    lines.append("=" * 60)
    lines.append("")

    if path.exists():
        content = path.read_text(encoding="utf-8").strip()
        lines.append(content)
    else:
        lines.append(f"Missing file: {path}")

    lines.append("")
    lines.append("")

master_text = "\n".join(lines).strip() + "\n"
master_summary_path.write_text(master_text, encoding="utf-8")

print(master_text)
print("Saved to:", master_summary_path)