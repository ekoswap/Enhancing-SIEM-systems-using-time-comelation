from pathlib import Path
import pandas as pd

pd.set_option("display.max_columns", None)

files = [
    "dns_redteam_zone_chunk_1.txt",
    "dns_redteam_zone_chunk_2.txt",
    "dns_redteam_zone_chunk_3.txt",
]

redteam_path = Path("data/raw/redteam.txt")
redteam_df = pd.read_csv(
    redteam_path,
    header=None,
    names=["timestamp", "user", "src_computer", "dst_computer"]
)

redteam_src = set(redteam_df["src_computer"].astype(str).unique())
redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())
redteam_all = redteam_src | redteam_dst

results = []

for file_name in files:
    file_path = Path("data/raw") / file_name

    dns_df = pd.read_csv(
        file_path,
        header=None,
        names=["time", "source_computer", "destination_computer"]
    )

    def mark_related(df):
        working = df.copy()
        working["redteam_src_match"] = working["source_computer"].astype(str).isin(redteam_all)
        working["redteam_dst_match"] = working["destination_computer"].astype(str).isin(redteam_all)
        working["redteam_related"] = working["redteam_src_match"] | working["redteam_dst_match"]
        return working

    baseline_df = mark_related(dns_df)

    agg_df = (
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

    def summarize(df):
        total = len(df)
        related = int(df["redteam_related"].sum())
        pct = (related / total * 100) if total else 0
        return total, round(pct, 2)

    baseline_total, baseline_pct = summarize(baseline_df)
    agg_total, agg_pct = summarize(agg_df)

    results.append({
        "file_name": file_name,
        "baseline_total": baseline_total,
        "baseline_redteam_pct": baseline_pct,
        "agg_total": agg_total,
        "agg_redteam_pct": agg_pct,
    })

results_df = pd.DataFrame(results)
print(results_df)

output_path = Path("outputs/dns_best_strategy_3chunks.csv")
results_df.to_csv(output_path, index=False)

print("\nSaved to:", output_path)