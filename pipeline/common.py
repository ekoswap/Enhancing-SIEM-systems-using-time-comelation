from __future__ import annotations

from pathlib import Path
import pandas as pd


def load_csv(path: str | Path, column_names: list[str]) -> pd.DataFrame:
    return pd.read_csv(path, header=None, names=column_names)


def mark_redteam_related(
    df: pd.DataFrame,
    src_col: str,
    dst_col: str,
    redteam_src: set[str],
    redteam_dst: set[str],
) -> pd.DataFrame:
    working = df.copy()
    redteam_all = redteam_src | redteam_dst

    working["redteam_src_match"] = working[src_col].astype(str).isin(redteam_all)
    working["redteam_dst_match"] = working[dst_col].astype(str).isin(redteam_all)
    working["redteam_related"] = working["redteam_src_match"] | working["redteam_dst_match"]

    return working


def summarize_redteam_ratio(
    df: pd.DataFrame,
    related_col: str = "redteam_related",
) -> dict:
    total = len(df)
    related = int(df[related_col].sum()) if total else 0
    pct = (related / total * 100) if total else 0.0

    return {
        "total_rows": total,
        "redteam_related_rows": related,
        "redteam_related_pct": round(pct, 2),
    }


def aggregate_with_max_flag(
    df: pd.DataFrame,
    group_cols: list[str],
    time_col: str,
    extra_aggs: dict[str, tuple[str, str]] | None = None,
) -> pd.DataFrame:
    agg_spec = {
        "start_time": (time_col, "min"),
        "end_time": (time_col, "max"),
        "event_count": (time_col, "size"),
        "redteam_related": ("redteam_related", "max"),
    }

    if extra_aggs:
        agg_spec.update(extra_aggs)

    grouped = (
        df.groupby(group_cols, dropna=False)
        .agg(**agg_spec)
        .reset_index()
    )

    return grouped
    
def mark_redteam_related_multi(
    df: pd.DataFrame,
    match_columns: list[str],
    redteam_values: set[str],
) -> pd.DataFrame:
    working = df.copy()

    related_mask = False
    for col in match_columns:
        current_mask = working[col].astype(str).isin(redteam_values)
        working[f"{col}_redteam_match"] = current_mask
        related_mask = related_mask | current_mask

    working["redteam_related"] = related_mask
    return working    