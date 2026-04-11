from pathlib import Path
import pandas as pd

# ---------- Files ----------
auth_path = Path("data/raw/auth_sample.txt")
redteam_path = Path("data/raw/redteam.txt")

# ---------- Load auth ----------
auth_df = pd.read_csv(
    auth_path,
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

# ---------- Load redteam ----------
redteam_df = pd.read_csv(
    redteam_path,
    header=None,
    names=[
        "timestamp",
        "user",
        "src_computer",
        "dst_computer"
    ]
)

print("=" * 50)
print("AUTH SAMPLE OVERVIEW")
print("=" * 50)
print("Shape:", auth_df.shape)
print("\nColumns:")
print(auth_df.columns.tolist())

print("\nMissing values:")
print(auth_df.isna().sum())

print("\nFirst 5 rows:")
print(auth_df.head())

print("\nTimestamp range:")
print("min =", auth_df["timestamp"].min())
print("max =", auth_df["timestamp"].max())

print("\nUnique values:")
for col in ["auth_type", "logon_type", "auth_orientation", "result"]:
    print(f"{col}: {auth_df[col].nunique()} unique")
    print(sorted(auth_df[col].astype(str).unique())[:20])
    print()

print("Duplicate rows:", auth_df.duplicated().sum())

print("\n" + "=" * 50)
print("REDTEAM OVERVIEW")
print("=" * 50)
print("Shape:", redteam_df.shape)

print("\nColumns:")
print(redteam_df.columns.tolist())

print("\nMissing values:")
print(redteam_df.isna().sum())

print("\nFirst 5 rows:")
print(redteam_df.head())

print("\nTimestamp range:")
print("min =", redteam_df["timestamp"].min())
print("max =", redteam_df["timestamp"].max())

print("\nDuplicate rows:", redteam_df.duplicated().sum())