from pathlib import Path
import sys

src = Path(r"F:\auth.txt")

if len(sys.argv) != 3:
    print("Usage: python make_auth_chunk.py <start_line> <output_name>")
    sys.exit(1)

start_line = int(sys.argv[1])
output_name = sys.argv[2]
dst = Path("data/raw") / output_name

num_lines = 10000

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