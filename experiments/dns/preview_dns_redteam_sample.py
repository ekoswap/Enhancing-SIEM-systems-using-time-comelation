from pathlib import Path
import pandas as pd

file_path = Path("data/raw/dns_redteam_sample.txt")

print("Exists:", file_path.exists())

if file_path.exists():
    df = pd.read_csv(
        file_path,
        header=None,
        names=["timestamp", "src_computer", "dst_computer"]
    )

    print("\nShape:")
    print(df.shape)

    print("\nTimestamp range:")
    print("min =", df["timestamp"].min())
    print("max =", df["timestamp"].max())

    print("\nHead:")
    print(df.head())
else:
    print("dns_redteam_sample not found")