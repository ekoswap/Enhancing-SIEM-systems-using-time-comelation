from pathlib import Path
import pandas as pd

file_path = Path("data/raw/redteam.txt")

print("Exists:", file_path.exists())

if file_path.exists():
    df = pd.read_csv(
        file_path,
        header=None,
        names=["timestamp", "user", "src_computer", "dst_computer"]
    )

    print("\nShape:")
    print(df.shape)

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nHead:")
    print(df.head())

    print("\nInfo:")
    df.info()
else:
    print("redteam file not found")