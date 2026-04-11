from pathlib import Path
import pandas as pd

file_path = Path("data/raw/auth_sample.txt")

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

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nHead:")
    print(df.head())

    print("\nInfo:")
    df.info()
else:
    print("auth_sample file not found")