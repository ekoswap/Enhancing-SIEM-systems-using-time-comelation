from pathlib import Path

RAW_DIR = Path("data/raw")

print("Available files/folders in data/raw:")
for item in RAW_DIR.iterdir():
    print("-", item.name)

print("\nDone.")