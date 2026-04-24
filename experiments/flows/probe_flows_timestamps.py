from pathlib import Path

file_path = Path(r"data/raw/flows.txt")

start_line = 16_000_000
check_every = 250_000
max_checks = 6

print("Exists:", file_path.exists())

if file_path.exists():
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for _ in range(start_line):
            next(f, None)

        current_line = start_line
        checks_done = 0

        for line in f:
            current_line += 1
            if current_line % check_every == 0:
                first_field = line.split(",")[0].strip()
                print(f"line {current_line}: time = {first_field}")
                checks_done += 1
                if checks_done >= max_checks:
                    break
else:
    print("flows file not found")