from pathlib import Path
import pandas as pd

flows_path = Path("data/raw/flows_redteam_sample.txt")
redteam_path = Path("data/raw/redteam.txt")

flows_df = pd.read_csv(
    flows_path,
    header=None,
    names=[
        "time",
        "duration",
        "source_computer",
        "source_port",
        "destination_computer",
        "destination_port",
        "protocol",
        "packet_count",
        "byte_count",
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

redteam_src = set(redteam_df["src_computer"].astype(str).unique())
redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())
redteam_all = redteam_src | redteam_dst


def mark_redteam_related(df: pd.DataFrame) -> pd.DataFrame:
    working_df = df.copy()
    working_df["redteam_src_match"] = working_df["source_computer"].astype(str).isin(redteam_all)
    working_df["redteam_dst_match"] = working_df["destination_computer"].astype(str).isin(redteam_all)
    working_df["redteam_related"] = (
        working_df["redteam_src_match"] |
        working_df["redteam_dst_match"]
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


def aggregate_flows(df: pd.DataFrame, group_cols: list[str], label: str):
    grouped = (
        df.groupby(group_cols, dropna=False)
        .agg(
            start_time=("time", "min"),
            end_time=("time", "max"),
            flow_count=("time", "size"),
            total_packets=("packet_count", "sum"),
            total_bytes=("byte_count", "sum"),
            redteam_related=("redteam_related", "max"),
        )
        .reset_index()
    )
    return summarize_stage(label, grouped)


base_df = mark_redteam_related(flows_df)

results = []

# Strategy 0: baseline
results.append(summarize_stage("baseline_raw", base_df))

# Strategy 1: dedup only
dedup_df = base_df.drop_duplicates().copy()
results.append(summarize_stage("dedup_only", dedup_df))

# Strategy 2: aggregate by source/destination/protocol
results.append(
    aggregate_flows(
        dedup_df,
        ["source_computer", "destination_computer", "protocol"],
        "agg_src_dst_protocol"
    )
)

# Strategy 3: filter protocol 6 only, then aggregate
proto6_df = dedup_df[dedup_df["protocol"] == 6].copy()
results.append(summarize_stage("protocol6_only", proto6_df))
results.append(
    aggregate_flows(
        proto6_df,
        ["source_computer", "destination_computer", "protocol"],
        "protocol6__agg_src_dst_protocol"
    )
)

# Strategy 4: filter duration > 0, then aggregate
dur_df = dedup_df[dedup_df["duration"] > 0].copy()
results.append(summarize_stage("duration_gt_0", dur_df))
results.append(
    aggregate_flows(
        dur_df,
        ["source_computer", "destination_computer", "protocol"],
        "duration_gt_0__agg_src_dst_protocol"
    )
)

# Strategy 5: protocol 6 AND duration > 0
combo_df = dedup_df[(dedup_df["protocol"] == 6) & (dedup_df["duration"] > 0)].copy()
results.append(summarize_stage("protocol6_and_duration_gt_0", combo_df))
results.append(
    aggregate_flows(
        combo_df,
        ["source_computer", "destination_computer", "protocol"],
        "protocol6_and_duration_gt_0__agg_src_dst_protocol"
    )
)

results_df = pd.DataFrame(results)
print(results_df)

output_path = Path("outputs/compare_flows_strategies.csv")
results_df.to_csv(output_path, index=False)

print("\nSaved to:", output_path)