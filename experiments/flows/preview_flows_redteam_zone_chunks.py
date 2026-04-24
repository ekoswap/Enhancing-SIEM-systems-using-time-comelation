from pathlib import Path
import pandas as pd

files = [
    "flows_redteam_zone_chunk_1.txt",
    "flows_redteam_zone_chunk_2.txt",
    "flows_redteam_zone_chunk_3.txt",
    "flows_redteam_zone_chunk_4.txt",
    "flows_redteam_zone_chunk_5.txt",
    "flows_redteam_zone_chunk_6.txt",
    "flows_redteam_zone_chunk_7.txt",
    "flows_redteam_zone_chunk_8.txt",
]

for file_name in files:
    file_path = Path("data/raw") / file_name

    print("=" * 60)
    print(file_name)
    print("=" * 60)

    if not file_path.exists():
        print("File not found\n")
        continue

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

    print("Shape:", df.shape)
    print("Time range:", df["time"].min(), "->", df["time"].max())
    print("Protocol values:")
    print(df["protocol"].value_counts())
    print()