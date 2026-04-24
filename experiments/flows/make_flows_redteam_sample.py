from pathlib import Path

src = Path(r"data/raw/flows.txt")
dst = Path("data/raw/flows_redteam_sample.txt")

start_line = 16_710_000
num_lines = 10_000

with open(src, "r", encoding="utf-8", errors="ignore") as fin:
    for _ in range(start_line):
        next(fin, None)

    with open(dst, "w", encoding="utf-8") as fout:
        for i, line in enumerate(fin):
            fout.write(line)
            if i + 1 >= num_lines:
                break

print("Done")
print("Saved to:", dst.resolve())
print("Exists:", dst.exists())
print("Size:", dst.stat().st_size if dst.exists() else "NOT FOUND")