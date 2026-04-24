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

    print("\n" + "=" * 60)
    print("TIME OVERLAP")
    print("=" * 60)
    print("FLOWS SAMPLE range:", flows_df["time"].min(), "->", flows_df["time"].max())
    print("REDTEAM range:", redteam_df["timestamp"].min(), "->", redteam_df["timestamp"].max())

    flows_in_redteam_range = flows_df[
        (flows_df["time"] >= redteam_df["timestamp"].min()) &
        (flows_df["time"] <= redteam_df["timestamp"].max())
    ]

    print("Flows rows inside redteam time range:", len(flows_in_redteam_range))

    print("\n" + "=" * 60)
    print("COMPUTER OVERLAP")
    print("=" * 60)

    flows_src = set(flows_df["source_computer"].astype(str).unique())
    flows_dst = set(flows_df["destination_computer"].astype(str).unique())
    flows_all = flows_src | flows_dst

    redteam_src = set(redteam_df["src_computer"].astype(str).unique())
    redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())
    redteam_all = redteam_src | redteam_dst

    common_computers = sorted(flows_all & redteam_all)

    print("Unique flows computers:", len(flows_all))
    print("Unique redteam computers:", len(redteam_all))
    print("Common computers:", len(common_computers))
    print("Example common computers:", common_computers[:30])

    print("\n" + "=" * 60)
    print("DIRECT PAIR MATCHES")
    print("=" * 60)

    pair_match_df = flows_df.merge(
        redteam_df,
        left_on=["source_computer", "destination_computer"],
        right_on=["src_computer", "dst_computer"],
        how="inner",
        suffixes=("_flows", "_red")
    )

    if not pair_match_df.empty:
        pair_match_df["time_diff"] = pair_match_df["time"] - pair_match_df["timestamp"]

    print("Direct pair matches:", len(pair_match_df))

    if not pair_match_df.empty:
        print("\nFirst 10 pair matches:")
        print(pair_match_df[
            ["time", "timestamp", "time_diff", "source_computer", "destination_computer"]
        ].head(10))
    else:
        print("No direct pair matches found.")
else:
    print("One or both files are missing.")