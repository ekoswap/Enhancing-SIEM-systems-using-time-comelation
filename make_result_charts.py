from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

output_dir = Path("outputs")
charts_dir = output_dir / "charts"
charts_dir.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load files
# -----------------------------
auth_zone_df = pd.read_csv(output_dir / "redteam_zone_experiment.csv")
auth_pres_df = pd.read_csv(output_dir / "redteam_zone_preservation.csv")
proc_df = pd.read_csv(output_dir / "proc_end_only_3chunks.csv")  # هذا صار فيه 8 chunks عندك

# -----------------------------
# Helper labels
# -----------------------------
auth_labels = [f"A{i}" for i in range(1, len(auth_zone_df) + 1)]
proc_labels = [f"P{i}" for i in range(1, len(proc_df) + 1)]

# -----------------------------
# Chart 1: Auth event reduction
# -----------------------------
plt.figure(figsize=(10, 5))
plt.bar(auth_labels, auth_zone_df["event_reduction_pct"])
plt.xlabel("Auth redteam-zone chunks")
plt.ylabel("Event reduction %")
plt.title("Auth: Event reduction across redteam-zone chunks")
plt.tight_layout()
plt.savefig(charts_dir / "auth_event_reduction.png", dpi=180)
plt.close()

# -----------------------------
# Chart 2: Auth correlation change
# -----------------------------
plt.figure(figsize=(10, 5))
plt.plot(auth_labels, auth_zone_df["correlation_change_pct"], marker="o")
plt.axhline(0, linewidth=1)
plt.xlabel("Auth redteam-zone chunks")
plt.ylabel("Correlation change %")
plt.title("Auth: Correlation change after pre-correlation")
plt.tight_layout()
plt.savefig(charts_dir / "auth_correlation_change.png", dpi=180)
plt.close()

# -----------------------------
# Chart 3: Auth redteam preservation
# -----------------------------
plt.figure(figsize=(10, 5))
plt.plot(auth_labels, auth_pres_df["normalized_redteam_pct"], marker="o", label="Before aggregation")
plt.plot(auth_labels, auth_pres_df["aggregated_redteam_pct"], marker="o", label="After aggregation")
plt.xlabel("Auth redteam-zone chunks")
plt.ylabel("Redteam-related %")
plt.title("Auth: Redteam-context preservation")
plt.legend()
plt.tight_layout()
plt.savefig(charts_dir / "auth_redteam_preservation.png", dpi=180)
plt.close()

# -----------------------------
# Chart 4: PROC exploratory comparison
# -----------------------------
plt.figure(figsize=(10, 5))
plt.plot(proc_labels, proc_df["baseline_redteam_pct"], marker="o", label="Baseline")
plt.plot(proc_labels, proc_df["end_only_redteam_pct"], marker="o", label="End-only")
plt.plot(proc_labels, proc_df["agg_redteam_pct"], marker="o", label="End-only + aggregation")
plt.xlabel("Proc redteam-zone chunks")
plt.ylabel("Redteam-related %")
plt.title("PROC: Baseline vs End-only strategies")
plt.legend()
plt.tight_layout()
plt.savefig(charts_dir / "proc_end_only_comparison.png", dpi=180)
plt.close()

print("Charts created successfully in:")
print(charts_dir.resolve())
print("\nFiles:")
for p in charts_dir.iterdir():
    print("-", p.name)