from pathlib import Path
import pandas as pd


def load_auth_sample(file_path: str | Path) -> pd.DataFrame:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(
        path,
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
    return df


def load_redteam(file_path: str | Path) -> pd.DataFrame:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(
        path,
        header=None,
        names=[
            "timestamp",
            "user",
            "src_computer",
            "dst_computer"
        ]
    )
    return df