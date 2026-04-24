from pathlib import Path
import pandas as pd

proc_path = Path("data/raw/proc_redteam_sample.txt")
redteam_path = Path("data/raw/redteam.txt")

print("PROC SAMPLE exists:", proc_path.exists())
print("REDTEAM exists:", redteam_path.exists())

if proc_path.exists() and redteam_path.exists():
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

    # -------------------------
    # Mark redteam-related rows
    # -------------------------
    proc_df["redteam_user_match"] = proc_df["user"].astype(str).isin(redteam_users)
    proc_df["redteam_computer_match"] = proc_df["computer"].astype(str).isin(redteam_all_computers)

    proc_df["redteam_related"] = (
        proc_df["redteam_user_match"] |
        proc_df["redteam_computer_match"]
    )

    # -------------------------
    # Simple filtering:
    # keep only Start events
    # -------------------------
    filtered_df = proc_df[proc_df["event_state"] == "Start"].copy()

    filtered_df["redteam_user_match"] = filtered_df["user"].astype(str).isin(redteam_users)
    filtered_df["redteam_computer_match"] = filtered_df["computer"].astype(str).isin(redteam_all_computers)
    filtered_df["redteam_related"] = (
        filtered_df["redteam_user_match"] |
        filtered_df["redteam_computer_match"]
    )

    # -------------------------
    # Simple aggregation:
    # group by user + computer + process
    # summarize over time
    # -------------------------
    aggregated_df = (
        filtered_df
        .groupby(["user", "computer", "process"], dropna=False)
        .agg(
            start_time=("timestamp", "min"),
            end_time=("timestamp", "max"),
            event_count=("timestamp", "size"),
            redteam_related=("redteam_related", "max"),
        )
        .reset_index()
    )

    def report(name, df, related_col="redteam_related"):
        total = len(df)
        related = int(df[related_col].sum())
        pct = (related / total * 100) if total else 0

        print("\n" + "=" * 60)
        print(name)
        print("=" * 60)
        print("Total rows:", total)
        print("Redteam-related rows:", related)
        print(f"Redteam-related %: {pct:.2f}%")

    report("PROC NORMALIZED SAMPLE", proc_df)
    report("PROC FILTERED SAMPLE (Start only)", filtered_df)
    report("PROC AGGREGATED SAMPLE", aggregated_df)

    print("\nTop event_state values before filtering:")
    print(proc_df["event_state"].value_counts())

    print("\nTop aggregated event_count values:")
    print(aggregated_df["event_count"].value_counts().head(10))
else:
    print("One or both files are missing.")