from pathlib import Path
import pandas as pd

pd.set_option("display.max_columns", None)

files = [
    "flows_redteam_zone_chunk_1.txt",
    "flows_redteam_zone_chunk_2.txt",
    "flows_redteam_zone_chunk_3.txt",
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

redteam_src = set(redteam_df["src_computer"].astype(str).unique())
redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())
redteam_all = redteam_src | redteam_dst

results = []

for file_name in files:
    file_path = Path("data/raw") / file_name

    flows_df = pd.read_csv(
        file_path,
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

    # baseline
    flows_df["redteam_src_match"] = flows_df["source_computer"].astype(str).isin(redteam_all)
    flows_df["redteam_dst_match"] = flows_df["destination_computer"].astype(str).isin(redteam_all)
    flows_df["redteam_related"] = flows_df["redteam_src_match"] | flows_df["redteam_dst_match"]

    # best strategy
    best_df = flows_df[
        (flows_df["protocol"] == 6) &
        (flows_df["duration"] > 0)
    ].copy()

    best_df["redteam_src_match"] = best_df["source_computer"].astype(str).isin(redteam_all)
    best_df["redteam_dst_match"] = best_df["destination_computer"].astype(str).isin(redteam_all)
    best_df["redteam_related"] = best_df["redteam_src_match"] | best_df["redteam_dst_match"]

    # aggregated best strategy
    agg_df = (
        best_df.groupby(
            ["source_computer", "destination_computer", "protocol"],
            dropna=False
        )
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

    baseline_total = len(flows_df)
    baseline_related = int(flows_df["redteam_related"].sum())
    baseline_pct = (baseline_related / baseline_total * 100) if baseline_total else 0

    best_total = len(best_df)
    best_related = int(best_df["redteam_related"].sum())
    best_pct = (best_related / best_total * 100) if best_total else 0

    agg_total = len(agg_df)
    agg_related = int(agg_df["redteam_related"].sum())
    agg_pct = (agg_related / agg_total * 100) if agg_total else 0

    results.append({
        "file_name": file_name,
        "baseline_total": baseline_total,
        "baseline_redteam_pct": round(baseline_pct, 2),
        "best_total": best_total,
        "best_redteam_pct": round(best_pct, 2),
        "agg_total": agg_total,
        "agg_redteam_pct": round(agg_pct, 2),
    })

results_df = pd.DataFrame(results)
print(results_df)

output_path = Path("outputs/flows_best_strategy_3chunks.csv")
results_df.to_csv(output_path, index=False)

print("\nSaved to:", output_path)