from pathlib import Path
import pandas as pd

file_path = Path("data/raw/flows.txt")

print("Exists:", file_path.exists())

if file_path.exists():
    df = pd.read_csv(
        file_path,
        header=None,
        nrows=10000,
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

    print("\nShape:")
    print(df.shape)

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nHead:")
    print(df.head())

    print("\nInfo:")
    df.info()

    print("\nMissing values:")
    print(df.isna().sum())

    print("\nTime range:")
    print("min =", df["time"].min())
    print("max =", df["time"].max())

    print("\nUnique counts per column:")
    for col in df.columns:
        print(f"{col} unique =", df[col].nunique())
else:
    print("flows file not found")