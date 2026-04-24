from pathlib import Path
import pandas as pd

file_path = Path("data/raw/proc.txt")

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

    print("\nUnique counts per column:")
    for col in df.columns:
        print(f"col {col} unique =", df[col].nunique())
else:
    print("proc file not found")