import pandas as pd


def split_user_domain(value: str) -> tuple[str, str]:
    """
    Split values like U620@DOM1 into:
    user_name = U620
    user_domain = DOM1

    If @ does not exist, keep the value in user_name and set domain to UNKNOWN.
    """
    value = str(value).strip()

    if "@" in value:
        user_name, user_domain = value.split("@", 1)
        return user_name, user_domain

    return value, "UNKNOWN"


def normalize_auth_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize LANL auth data into a cleaner structure for the project.
    """
    working_df = df.copy()

    # Ensure timestamp is numeric
    working_df["timestamp"] = pd.to_numeric(working_df["timestamp"], errors="coerce")

    # Split source user
    src_split = working_df["src_user"].apply(split_user_domain)
    working_df["src_user_name"] = src_split.apply(lambda x: x[0])
    working_df["src_user_domain"] = src_split.apply(lambda x: x[1])

    # Split destination user
    dst_split = working_df["dst_user"].apply(split_user_domain)
    working_df["dst_user_name"] = dst_split.apply(lambda x: x[0])
    working_df["dst_user_domain"] = dst_split.apply(lambda x: x[1])

    # Create a simplified event type
    working_df["event_type"] = (
        working_df["auth_orientation"].astype(str).str.strip()
        + "_"
        + working_df["result"].astype(str).str.strip()
    )

    # Reorder useful columns
    normalized_df = working_df[
        [
            "timestamp",
            "src_user",
            "src_user_name",
            "src_user_domain",
            "dst_user",
            "dst_user_name",
            "dst_user_domain",
            "src_computer",
            "dst_computer",
            "auth_type",
            "logon_type",
            "auth_orientation",
            "result",
            "event_type",
        ]
    ].copy()

    # Sort by time
    normalized_df = normalized_df.sort_values("timestamp").reset_index(drop=True)

    return normalized_df