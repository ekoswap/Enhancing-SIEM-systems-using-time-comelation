import pandas as pd


def filter_auth_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Improved filtering for LANL auth events.

    Rules:
    1) remove rows with missing timestamp
    2) remove ScreenLock_Success
    3) remove AuthMap_Success
    4) remove rows where src_user starts with ANONYMOUS LOGON
    """
    working_df = df.copy()

    # Remove missing timestamps
    working_df = working_df.dropna(subset=["timestamp"])

    # Remove low-value event types
    excluded_event_types = {
        "ScreenLock_Success",
        "AuthMap_Success",
    }
    working_df = working_df[~working_df["event_type"].isin(excluded_event_types)]

    # Remove anonymous logon rows
    working_df = working_df[
        ~working_df["src_user"].astype(str).str.startswith("ANONYMOUS LOGON")
    ]

    working_df = working_df.reset_index(drop=True)
    return working_df