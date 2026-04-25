from pathlib import Path

output_dir = Path("outputs")

pipeline_summary_path = output_dir / "pipeline_summary.txt"
multi_chunk_summary_path = output_dir / "multi_chunk_experiment_summary.txt"
decision_summary_path = output_dir / "final_experiment_decision_summary.txt"
redteam_summary_path = output_dir / "redteam_overlap_summary.txt"
redteam_preservation_summary_path = output_dir / "redteam_preservation_multi_chunk_summary.txt"
redteam_zone_experiment_summary_path = output_dir / "redteam_zone_experiment_summary.txt"
redteam_zone_preservation_summary_path = output_dir / "redteam_zone_preservation_summary.txt"
proc_exploration_summary_path = output_dir / "proc_exploratory_summary.txt"
proc_end_only_summary_path = output_dir / "proc_end_only_8chunks_summary.txt"
dns_decision_summary_path = output_dir / "dns_decision_summary.txt"
flows_exploration_summary_path = output_dir / "flows_exploratory_summary.txt"
flows_8chunks_summary_path = output_dir / "flows_8chunks_summary.txt"
dns_3chunks_summary_path = output_dir / "dns_3chunks_summary.txt"

master_summary_path = output_dir / "master_project_summary.txt"

sections = [
    ("Pipeline Summary", pipeline_summary_path),
    ("Multi-Chunk Experiment Summary", multi_chunk_summary_path),
    ("Final Experiment Decision Summary", decision_summary_path),
    ("Redteam Overlap Summary", redteam_summary_path),
    ("Redteam Preservation Multi-Chunk Summary", redteam_preservation_summary_path),
    ("Redteam-Zone Experiment Summary", redteam_zone_experiment_summary_path),
    ("Redteam-Zone Preservation Summary", redteam_zone_preservation_summary_path),
    ("PROC Exploratory Summary", proc_exploration_summary_path),
    ("PROC End-Only 8-Chunks Summary", proc_end_only_summary_path),
    ("DNS Decision Summary", dns_decision_summary_path),
    ("FLOWS 8-Chunks Summary", flows_8chunks_summary_path),
    ("DNS 3-Chunks Summary", dns_3chunks_summary_path),
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