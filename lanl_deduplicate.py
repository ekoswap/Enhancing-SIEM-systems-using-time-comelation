import pandas as pd


def deduplicate_auth_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove exact duplicate auth events based on the main normalized columns.
    """
    working_df = df.copy()

    dedup_columns = [
        "timestamp",
        "src_user",
        "dst_user",
        "src_computer",
        "dst_computer",
        "auth_type",
        "logon_type",
        "auth_orientation",
        "result",
        "event_type",
    ]

    deduplicated_df = working_df.drop_duplicates(subset=dedup_columns).reset_index(drop=True)

    return deduplicated_df