from pathlib import Path
import pandas as pd

file_path = Path("data/raw/proc.txt")

print("Exists:", file_path.exists())

if file_path.exists():
    df = pd.read_csv(
        file_path,
        header=None,
        nrows=10000,
        names=[
            "timestamp",
            "user",
            "computer",
            "process",
            "event_state"
        ]
    )

    print("\nShape:")
    print(df.shape)

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nHead:")
    print(df.head())

    print("\nMissing values:")
    print(df.isna().sum())

    print("\nTimestamp range:")
    print("min =", df["timestamp"].min())
    print("max =", df["timestamp"].max())

    print("\nUnique event_state values:")
    print(df["event_state"].value_counts())

    print("\nTop processes:")
    print(df["process"].value_counts().head(20))

    print("\nSample users:")
    print(df["user"].drop_duplicates().head(20).tolist())

    print("\nSample computers:")
    print(df["computer"].drop_duplicates().head(20).tolist())
else:
    print("proc file not found")