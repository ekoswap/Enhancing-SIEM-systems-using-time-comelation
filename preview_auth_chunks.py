from pathlib import Path
import pandas as pd

files = [
    "auth_sample_late.txt",
    "auth_sample_chunk2.txt",
    "auth_sample_chunk3.txt",
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