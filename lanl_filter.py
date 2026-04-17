import pandas as pd


def filter_auth_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep more meaningful auth events for the project.
    Current rules:
    1) remove rows with missing timestamp
    2) remove rows where event_type is ScreenLock_Success
    3) keep all success/fail auth events otherwise
    """
    working_df = df.copy()

    # Remove missing timestamps
    working_df = working_df.dropna(subset=["timestamp"])

    # Remove very rare low-value event
    working_df = working_df[working_df["event_type"] != "ScreenLock_Success"]

    # Reset index
    working_df = working_df.reset_index(drop=True)

    return working_df