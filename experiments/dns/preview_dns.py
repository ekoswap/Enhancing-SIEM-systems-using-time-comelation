from pathlib import Path
import pandas as pd

file_path = Path("data/raw/dns.txt")

print("Exists:", file_path.exists())

if file_path.exists():
    df = pd.read_csv(file_path, header=None, nrows=10000)

    print("\nShape:")
    print(df.shape)

    print("\nHead:")
    print(df.head())

    print("\nInfo:")
    df.info()

    print("\nMissing values:")
    print(df.isna().sum())

    print("\nTimestamp range:")
    print("min =", df[0].min())
    print("max =", df[0].max())

    print("\nUnique counts:")
    print("col 1 unique =", df[1].nunique())
    print("col 2 unique =", df[2].nunique())
else:
    print("dns file not found")