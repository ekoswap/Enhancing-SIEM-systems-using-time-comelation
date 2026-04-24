from pathlib import Path
import pandas as pd

file_path = Path("data/raw/proc_redteam_sample.txt")

print("Exists:", file_path.exists())

if file_path.exists():
    df = pd.read_csv(
        file_path,
        header=None,
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

    print("\nTimestamp range:")
    print("min =", df["timestamp"].min())
    print("max =", df["timestamp"].max())

    print("\nHead:")
    print(df.head())

    print("\nEvent states:")
    print(df["event_state"].value_counts())
else:
    print("proc_redteam_sample not found")