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

    best_df = flows_df[
        (flows_df["protocol"] == 6) &
        (flows_df["duration"] > 0)
    ].copy()

    best_df["redteam_src_match"] = best_df["source_computer"].astype(str).isin(redteam_all)
    best_df["redteam_dst_match"] = best_df["destination_computer"].astype(str).isin(redteam_all)
    best_df["redteam_related"] = best_df["redteam_src_match"] | best_df["redteam_dst_match"]

    # Aggregation key 1
    agg1 = (
        best_df.groupby(
            ["source_computer", "destination_computer", "protocol"],
            dropna=False
        )
        .agg(
            flow_count=("time", "size"),
            total_packets=("packet_count", "sum"),
            total_bytes=("byte_count", "sum"),
            redteam_related=("redteam_related", "max"),
        )
        .reset_index()
    )

    # Aggregation key 2
    agg2 = (
        best_df.groupby(
            ["source_computer", "destination_computer"],
            dropna=False
        )
        .agg(
            flow_count=("time", "size"),
            total_packets=("packet_count", "sum"),
            total_bytes=("byte_count", "sum"),
            redteam_related=("redteam_related", "max"),
        )
        .reset_index()
    )

    # Aggregation key 3
    agg3 = (
        best_df.groupby(
            ["destination_computer", "protocol"],
            dropna=False
        )
        .agg(
            flow_count=("time", "size"),
            total_packets=("packet_count", "sum"),
            total_bytes=("byte_count", "sum"),
            redteam_related=("redteam_related", "max"),
        )
        .reset_index()
    )

    def pct(df):
        return round(df["redteam_related"].sum() / len(df) * 100, 2) if len(df) else 0

    results.append({
        "file_name": file_name,
        "best_total": len(best_df),
        "best_redteam_pct": round(best_df["redteam_related"].sum() / len(best_df) * 100, 2),
        "agg_src_dst_protocol_total": len(agg1),
        "agg_src_dst_protocol_pct": pct(agg1),
        "agg_src_dst_total": len(agg2),
        "agg_src_dst_pct": pct(agg2),
        "agg_dst_protocol_total": len(agg3),
        "agg_dst_protocol_pct": pct(agg3),
    })

results_df = pd.DataFrame(results)
print(results_df)

output_path = Path("outputs/compare_flows_aggregation_keys.csv")
results_df.to_csv(output_path, index=False)

print("\nSaved to:", output_path)