from pathlib import Path
import pandas as pd

flows_path = Path("data/raw/flows_redteam_sample.txt")
redteam_path = Path("data/raw/redteam.txt")

print("FLOWS SAMPLE exists:", flows_path.exists())
print("REDTEAM exists:", redteam_path.exists())

if flows_path.exists() and redteam_path.exists():
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

    # -------------------------
    # Mark redteam-related rows
    # -------------------------
    flows_df["redteam_src_match"] = flows_df["source_computer"].astype(str).isin(redteam_all)
    flows_df["redteam_dst_match"] = flows_df["destination_computer"].astype(str).isin(redteam_all)
    flows_df["redteam_related"] = flows_df["redteam_src_match"] | flows_df["redteam_dst_match"]

    # -------------------------
    # Exact deduplication
    # -------------------------
    dedup_df = flows_df.drop_duplicates().copy()

    dedup_df["redteam_src_match"] = dedup_df["source_computer"].astype(str).isin(redteam_all)
    dedup_df["redteam_dst_match"] = dedup_df["destination_computer"].astype(str).isin(redteam_all)
    dedup_df["redteam_related"] = dedup_df["redteam_src_match"] | dedup_df["redteam_dst_match"]

    # -------------------------
    # Aggregation
    # -------------------------
    agg_df = (
        dedup_df.groupby(
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

    report("FLOWS BASELINE SAMPLE", flows_df)
    report("FLOWS DEDUPLICATED SAMPLE", dedup_df)
    report("FLOWS AGGREGATED SAMPLE", agg_df)

    print("\nDuplicate rows removed:", len(flows_df) - len(dedup_df))

    print("\nTop protocol values:")
    print(flows_df["protocol"].value_counts())

    print("\nTop aggregated flow_count values:")
    print(agg_df["flow_count"].value_counts().head(10))
else:
    print("One or both files are missing.")
    