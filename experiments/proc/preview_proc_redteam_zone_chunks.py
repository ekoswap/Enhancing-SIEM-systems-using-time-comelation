from pathlib import Path
import pandas as pd

files = [
    "proc_redteam_zone_chunk_1.txt",
    "proc_redteam_zone_chunk_2.txt",
    "proc_redteam_zone_chunk_3.txt",
    "proc_redteam_zone_chunk_4.txt",
    "proc_redteam_zone_chunk_5.txt",
    "proc_redteam_zone_chunk_6.txt",
    "proc_redteam_zone_chunk_7.txt",
    "proc_redteam_zone_chunk_8.txt",
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
            "timestamp",
            "user",
            "computer",
            "process",
            "event_state"
        ]
    )

    print("Shape:", df.shape)
    print("Timestamp range:", df["timestamp"].min(), "->", df["timestamp"].max())
    print("Event states:")
    print(df["event_state"].value_counts())
    print()