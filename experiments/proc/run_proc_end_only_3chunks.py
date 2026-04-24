from pathlib import Path
import pandas as pd

pd.set_option("display.max_columns", None)

files = [
    "proc_redteam_zone_chunk_1.txt",
    "proc_redteam_zone_chunk_2.txt",
    "proc_redteam_zone_chunk_3.txt",
    "proc_redteam_zone_chunk_4.txt",
    "proc_redteam_zone_chunk_5.txt",
    "proc_redteam_zone_chunk_6.txt",
    "proc_redteam_zone_chunk_7.txt",
    "proc_redteam_zone_chunk_8.txt",
]

redteam_path = Path("data/raw/redteam.txt")
redteam_df = pd.read_csv(
    redteam_path,
    header=None,
    names=[
        "timestamp",
        "user",
        "src_computer",
        "dst_computer"
    ]
)

redteam_users = set(redteam_df["user"].astype(str).unique())
redteam_src = set(redteam_df["src_computer"].astype(str).unique())
redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())
redteam_all_computers = redteam_src | redteam_dst

results = []

for file_name in files:
    file_path = Path("data/raw") / file_name

    proc_df = pd.read_csv(
        file_path,
        header=None,
        names=[
            "timestamp",
            "user",
            "computer",
            "process",
            "event_state"
        ]
    )

    # baseline marking
    proc_df["redteam_user_match"] = proc_df["user"].astype(str).isin(redteam_users)
    proc_df["redteam_computer_match"] = proc_df["computer"].astype(str).isin(redteam_all_computers)
    proc_df["redteam_related"] = (
        proc_df["redteam_user_match"] |
        proc_df["redteam_computer_match"]
    )

    # end_only
    end_df = proc_df[proc_df["event_state"] == "End"].copy()
    end_df["redteam_user_match"] = end_df["user"].astype(str).isin(redteam_users)
    end_df["redteam_computer_match"] = end_df["computer"].astype(str).isin(redteam_all_computers)
    end_df["redteam_related"] = (
        end_df["redteam_user_match"] |
        end_df["redteam_computer_match"]
    )

    # aggregated end_only
    agg_df = (
        end_df.groupby(["user", "computer", "process"], dropna=False)
        .agg(
            start_time=("timestamp", "min"),
            end_time=("timestamp", "max"),
            event_count=("timestamp", "size"),
            redteam_related=("redteam_related", "max"),
        )
        .reset_index()
    )

    baseline_total = len(proc_df)
    baseline_related = int(proc_df["redteam_related"].sum())
    baseline_pct = (baseline_related / baseline_total * 100) if baseline_total else 0

    end_total = len(end_df)
    end_related = int(end_df["redteam_related"].sum())
    end_pct = (end_related / end_total * 100) if end_total else 0

    agg_total = len(agg_df)
    agg_related = int(agg_df["redteam_related"].sum())
    agg_pct = (agg_related / agg_total * 100) if agg_total else 0

    results.append({
        "file_name": file_name,
        "baseline_total": baseline_total,
        "baseline_redteam_pct": round(baseline_pct, 2),
        "end_only_total": end_total,
        "end_only_redteam_pct": round(end_pct, 2),
        "agg_total": agg_total,
        "agg_redteam_pct": round(agg_pct, 2),
    })

results_df = pd.DataFrame(results)
print(results_df)

output_path = Path("outputs/proc_end_only_3chunks.csv")
results_df.to_csv(output_path, index=False)

print("\nSaved to:", output_path)