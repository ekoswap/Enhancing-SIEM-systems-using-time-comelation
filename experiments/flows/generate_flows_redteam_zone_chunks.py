from pathlib import Path

src = Path(r"data/raw/flows.txt")
output_dir = Path("data/raw")
output_dir.mkdir(parents=True, exist_ok=True)

base_start_line = 16_710_000

offsets = [
    0,
    25_000,
    50_000,
    75_000,
    100_000,
    125_000,
    150_000,
    175_000,
]
num_lines = 10_000

for idx, offset in enumerate(offsets, start=1):
    start_line = base_start_line + offset
    output_name = f"flows_redteam_zone_chunk_{idx}.txt"
    dst = output_dir / output_name

    with open(src, "r", encoding="utf-8", errors="ignore") as fin:
        for _ in range(start_line):
            next(fin, None)

        with open(dst, "w", encoding="utf-8") as fout:
            for i, line in enumerate(fin):
                fout.write(line)
                if i + 1 >= num_lines:
                    break

    print(f"Created: {output_name}")
    print(f"  start_line = {start_line}")
    print(f"  saved_to   = {dst.resolve()}")
    print(f"  exists     = {dst.exists()}")
    print(f"  size       = {dst.stat().st_size if dst.exists() else 'NOT FOUND'}")
    print("-" * 60)