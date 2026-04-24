from pathlib import Path
import pandas as pd

proc_path = Path("data/raw/proc_redteam_sample.txt")
redteam_path = Path("data/raw/redteam.txt")

proc_df = pd.read_csv(
    proc_path,
    header=None,
    names=[
        "timestamp",
        "user",
        "computer",
        "process",
        "event_state"
    ]
)

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


def mark_redteam_related(df: pd.DataFrame) -> pd.DataFrame:
    working_df = df.copy()
    working_df["redteam_user_match"] = working_df["user"].astype(str).isin(redteam_users)
    working_df["redteam_computer_match"] = working_df["computer"].astype(str).isin(redteam_all_computers)
    working_df["redteam_related"] = (
        working_df["redteam_user_match"] |
        working_df["redteam_computer_match"]
    )
    return working_df


def summarize_stage(label: str, df: pd.DataFrame, related_col: str = "redteam_related"):
    total = len(df)
    related = int(df[related_col].sum())
    pct = (related / total * 100) if total else 0
    return {
        "strategy": label,
        "total_rows": total,
        "redteam_related_rows": related,
        "redteam_related_pct": round(pct, 2),
    }


def aggregate_proc(df: pd.DataFrame, group_cols: list[str], label: str):
    grouped = (
        df.groupby(group_cols, dropna=False)
        .agg(
            start_time=("timestamp", "min"),
            end_time=("timestamp", "max"),
            event_count=("timestamp", "size"),
            redteam_related=("redteam_related", "max"),
        )
        .reset_index()
    )
    return summarize_stage(label, grouped)


base_df = mark_redteam_related(proc_df)

results = []

# Strategy 0: baseline, no filtering
results.append(summarize_stage("baseline_raw", base_df))

# Strategy 1: Start only
start_df = base_df[base_df["event_state"] == "Start"].copy()
results.append(summarize_stage("start_only", start_df))
results.append(aggregate_proc(start_df, ["user", "computer", "process"], "start_only__agg_user_computer_process"))
results.append(aggregate_proc(start_df, ["computer", "process"], "start_only__agg_computer_process"))

# Strategy 2: End only
end_df = base_df[base_df["event_state"] == "End"].copy()
results.append(summarize_stage("end_only", end_df))
results.append(aggregate_proc(end_df, ["user", "computer", "process"], "end_only__agg_user_computer_process"))
results.append(aggregate_proc(end_df, ["computer", "process"], "end_only__agg_computer_process"))

# Strategy 3: no filtering, aggregate
results.append(aggregate_proc(base_df, ["user", "computer", "process"], "all_events__agg_user_computer_process"))
results.append(aggregate_proc(base_df, ["computer", "process"], "all_events__agg_computer_process"))

results_df = pd.DataFrame(results)
print(results_df)

output_path = Path("outputs/compare_proc_strategies.csv")
results_df.to_csv(output_path, index=False)

print("\nSaved to:", output_path)