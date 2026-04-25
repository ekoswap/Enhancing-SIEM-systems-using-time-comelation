from pathlib import Path
import pandas as pd

dns_path = Path("data/raw/dns_redteam_sample.txt")
redteam_path = Path("data/raw/redteam.txt")

print("DNS SAMPLE exists:", dns_path.exists())
print("REDTEAM exists:", redteam_path.exists())

if dns_path.exists() and redteam_path.exists():
    dns_df = pd.read_csv(
        dns_path,
        header=None,
        names=[
            "time",
            "source_computer",
            "destination_computer",
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

    def mark_related(df):
        working = df.copy()
        working["redteam_src_match"] = working["source_computer"].astype(str).isin(redteam_all)
        working["redteam_dst_match"] = working["destination_computer"].astype(str).isin(redteam_all)
        working["redteam_related"] = working["redteam_src_match"] | working["redteam_dst_match"]
        return working

    baseline_df = mark_related(dns_df)

    agg_pair_df = (
        baseline_df.groupby(
            ["source_computer", "destination_computer"],
            dropna=False
        )
        .agg(
            start_time=("time", "min"),
            end_time=("time", "max"),
            event_count=("time", "size"),
            redteam_related=("redteam_related", "max"),
        )
        .reset_index()
    )

    agg_dst_df = (
        baseline_df.groupby(
            ["destination_computer"],
            dropna=False
        )
        .agg(
            start_time=("time", "min"),
            end_time=("time", "max"),
            event_count=("time", "size"),
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

    report("DNS BASELINE SAMPLE", baseline_df)
    report("DNS AGGREGATED BY SRC_DST", agg_pair_df)
    report("DNS AGGREGATED BY DST", agg_dst_df)

    print("\nTop event_count values for SRC_DST aggregation:")
    print(agg_pair_df["event_count"].value_counts().head(10))

    print("\nTop event_count values for DST aggregation:")
    print(agg_dst_df["event_count"].value_counts().head(10))
else:
    print("One or both files are missing.")