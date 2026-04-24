from pathlib import Path

flows_path = Path("data/raw/flows.txt")
redteam_path = Path("data/raw/redteam.txt")

# collect unique redteam pairs
redteam_pairs = set()

with open(redteam_path, "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        parts = line.strip().split(",")
        if len(parts) >= 4:
            src = parts[2].strip()
            dst = parts[3].strip()
            redteam_pairs.add((src, dst))

print("Unique redteam pairs:", len(redteam_pairs))
print("Searching flows for matching pairs...")

match_count = 0
first_matches = []

with open(flows_path, "r", encoding="utf-8", errors="ignore") as f:
    for i, line in enumerate(f, start=1):
        parts = line.strip().split(",")
        if len(parts) >= 5:
            src = parts[2].strip()
            dst = parts[4].strip()

            if (src, dst) in redteam_pairs:
                match_count += 1
                if len(first_matches) < 10:
                    first_matches.append((i, parts[0].strip(), src, dst))

print("Total pair matches found in full flows file:", match_count)

if first_matches:
    print("\nFirst 10 matches:")
    for row in first_matches:
        print(f"line {row[0]}: time={row[1]}, src={row[2]}, dst={row[3]}")
else:
    print("No pair matches found in full flows file.")