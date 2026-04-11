from pathlib import Path
import pandas as pd

file_path = Path("data/raw/auth_sample_late.txt")

print("Exists:", file_path.exists())

if file_path.exists():
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

    print("\nShape:")
    print(df.shape)

    print("\nTimestamp range:")
    print("min =", df["timestamp"].min())
    print("max =", df["timestamp"].max())

    print("\nHead:")
    print(df.head())

    print("\nDuplicate rows:")
    print(df.duplicated().sum())
else:
    print("auth_sample_late file not found")