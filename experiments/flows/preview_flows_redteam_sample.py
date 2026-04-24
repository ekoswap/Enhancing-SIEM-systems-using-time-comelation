from pathlib import Path
import pandas as pd

file_path = Path("data/raw/flows_redteam_sample.txt")

print("Exists:", file_path.exists())

if file_path.exists():
    df = pd.read_csv(
        file_path,
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

    print("\nShape:")
    print(df.shape)

    print("\nTime range:")
    print("min =", df["time"].min())
    print("max =", df["time"].max())

    print("\nHead:")
    print(df.head())
else:
    print("flows_redteam_sample not found")