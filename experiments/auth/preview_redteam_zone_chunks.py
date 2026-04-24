from pathlib import Path
import pandas as pd

files = [
    "auth_redteam_zone_chunk_1.txt",
    "auth_redteam_zone_chunk_2.txt",
    "auth_redteam_zone_chunk_3.txt",
    "auth_redteam_zone_chunk_4.txt",
    "auth_redteam_zone_chunk_5.txt",
    "auth_redteam_zone_chunk_6.txt",
    "auth_redteam_zone_chunk_7.txt",
    "auth_redteam_zone_chunk_8.txt",
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
            "src_user",
            "dst_user",
            "src_computer",
            "dst_computer",
            "auth_type",
            "logon_type",
            "auth_orientation",
            "result"
        ]
    )

    print("Shape:", df.shape)
    print("Timestamp range:", df["timestamp"].min(), "->", df["timestamp"].max())
    print("Duplicate rows:", df.duplicated().sum())
    print()