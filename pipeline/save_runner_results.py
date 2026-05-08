from __future__ import annotations

from pathlib import Path
import pandas as pd

from pipeline.generic_runner import run_all_sources


def main():
    results = run_all_sources()
    df = pd.DataFrame(results)

    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    output_path = output_dir / "pipeline_runner_results.csv"
    df.to_csv(output_path, index=False)

    print("\nSaved to:", output_path)
    print("\nResult table:")
    print(df)


if __name__ == "__main__":
    main()