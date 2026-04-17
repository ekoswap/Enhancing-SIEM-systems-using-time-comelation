from pathlib import Path

output_path = Path("outputs/final_experiment_decision_summary.txt")

lines = [
    "Final Experiment Decision Summary",
    "=" * 40,
    "",
    "Chosen filtering strategy:",
    "- Improved filter version",
    "- Removes ScreenLock_Success, AuthMap_Success, and ANONYMOUS LOGON rows",
    "",
    "Reason:",
    "- It cleans the event stream much more effectively than the old filter",
    "- It reduced low-value events while preserving the positive effect on downstream correlation",
    "",
    "Chosen aggregation strategy:",
    "- Relaxed temporal aggregation",
    "- Grouping key: src_user + dst_computer + event_type",
    "",
    "Chosen time window:",
    "- 5",
    "",
    "Reason:",
    "- The strict version was too restrictive and produced limited reduction",
    "- The relaxed version gave more meaningful stream compaction",
    "- Increasing the time window did not improve results, so 5 was kept as a simple and stable choice",
    "",
    "Observed results on the main chunk:",
    "- After filtering: 9519",
    "- After relaxed aggregation: 9439",
    "- Aggregation reduction: 80 (0.84%)",
    "- Correlation before: 360",
    "- Correlation after: 337",
    "- Correlation change: -23 (-6.39%)",
    "",
    "Observed results across 3 chunks:",
    "- Average event reduction: 0.74%",
    "- Average correlation reduction: -4.56%",
    "",
    "Final conclusion:",
    "The project direction is supported by repeated experimental results on LANL authentication chunks.",
    "The selected pre-correlation pipeline reduces the stream before baseline correlation and consistently decreases downstream correlation outputs.",
]

text = "\n".join(lines)
output_path.write_text(text, encoding="utf-8")

print(text)
print("\nSaved to:", output_path)