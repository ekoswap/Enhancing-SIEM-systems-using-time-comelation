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
            "timestamp",
            "src_computer",
            "dst_computer"
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
    print("DNS SAMPLE range:", dns_df["timestamp"].min(), "->", dns_df["timestamp"].max())
    print("REDTEAM range:", redteam_df["timestamp"].min(), "->", redteam_df["timestamp"].max())

    dns_in_redteam_range = dns_df[
        (dns_df["timestamp"] >= redteam_df["timestamp"].min()) &
        (dns_df["timestamp"] <= redteam_df["timestamp"].max())
    ]

    print("DNS rows inside redteam time range:", len(dns_in_redteam_range))

    print("\n" + "=" * 60)
    print("COMPUTER OVERLAP")
    print("=" * 60)

    dns_src = set(dns_df["src_computer"].astype(str).unique())
    dns_dst = set(dns_df["dst_computer"].astype(str).unique())
    dns_all = dns_src | dns_dst

    redteam_src = set(redteam_df["src_computer"].astype(str).unique())
    redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())
    redteam_all = redteam_src | redteam_dst

    common_computers = sorted(dns_all & redteam_all)

    print("Unique dns computers:", len(dns_all))
    print("Unique redteam computers:", len(redteam_all))
    print("Common computers:", len(common_computers))
    print("Example common computers:", common_computers[:30])

    print("\n" + "=" * 60)
    print("DIRECT PAIR MATCHES")
    print("=" * 60)

    pair_match_df = dns_df.merge(
        redteam_df,
        on=["src_computer", "dst_computer"],
        how="inner",
        suffixes=("_dns", "_red")
    )

    if not pair_match_df.empty:
        pair_match_df["time_diff"] = pair_match_df["timestamp_dns"] - pair_match_df["timestamp_red"]

    print("Direct pair matches:", len(pair_match_df))

    if not pair_match_df.empty:
        print("\nFirst 10 pair matches:")
        print(pair_match_df[
            ["timestamp_dns", "timestamp_red", "time_diff", "src_computer", "dst_computer"]
        ].head(10))
    else:
        print("No direct pair matches found.")
else:
    print("One or both files are missing.")