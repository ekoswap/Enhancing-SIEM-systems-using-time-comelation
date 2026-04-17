import pandas as pd


def temporal_aggregate_auth_events(df: pd.DataFrame, time_window: int = 5) -> pd.DataFrame:
    """
    Strict aggregation version:
    group events by:
    - src_user
    - dst_user
    - src_computer
    - dst_computer
    - event_type
    """
    working_df = df.copy()
    working_df = working_df.sort_values("timestamp").reset_index(drop=True)

    key_columns = [
        "src_user",
        "dst_user",
        "src_computer",
        "dst_computer",
        "event_type",
    ]

    aggregated_rows = []
    current_group = None
    group_id = 1

    for _, row in working_df.iterrows():
        row_key = tuple(row[col] for col in key_columns)
        row_time = row["timestamp"]

        if current_group is None:
            current_group = {
                "group_id": group_id,
                "start_time": row_time,
                "end_time": row_time,
                "event_count": 1,
                "src_user": row["src_user"],
                "dst_user": row["dst_user"],
                "src_computer": row["src_computer"],
                "dst_computer": row["dst_computer"],
                "event_type": row["event_type"],
                "auth_type": row["auth_type"],
                "logon_type": row["logon_type"],
                "result": row["result"],
                "key": row_key,
            }
            continue

        same_key = row_key == current_group["key"]
        within_window = (row_time - current_group["end_time"]) <= time_window

        if same_key and within_window:
            current_group["end_time"] = row_time
            current_group["event_count"] += 1
        else:
            aggregated_rows.append({
                "group_id": current_group["group_id"],
                "start_time": current_group["start_time"],
                "end_time": current_group["end_time"],
                "event_count": current_group["event_count"],
                "src_user": current_group["src_user"],
                "dst_user": current_group["dst_user"],
                "src_computer": current_group["src_computer"],
                "dst_computer": current_group["dst_computer"],
                "event_type": current_group["event_type"],
                "auth_type": current_group["auth_type"],
                "logon_type": current_group["logon_type"],
                "result": current_group["result"],
            })

            group_id += 1
            current_group = {
                "group_id": group_id,
                "start_time": row_time,
                "end_time": row_time,
                "event_count": 1,
                "src_user": row["src_user"],
                "dst_user": row["dst_user"],
                "src_computer": row["src_computer"],
                "dst_computer": row["dst_computer"],
                "event_type": row["event_type"],
                "auth_type": row["auth_type"],
                "logon_type": row["logon_type"],
                "result": row["result"],
                "key": row_key,
            }

    if current_group is not None:
        aggregated_rows.append({
            "group_id": current_group["group_id"],
            "start_time": current_group["start_time"],
            "end_time": current_group["end_time"],
            "event_count": current_group["event_count"],
            "src_user": current_group["src_user"],
            "dst_user": current_group["dst_user"],
            "src_computer": current_group["src_computer"],
            "dst_computer": current_group["dst_computer"],
            "event_type": current_group["event_type"],
            "auth_type": current_group["auth_type"],
            "logon_type": current_group["logon_type"],
            "result": current_group["result"],
        })

    aggregated_df = pd.DataFrame(aggregated_rows)
    return aggregated_df


def temporal_aggregate_auth_events_relaxed(df: pd.DataFrame, time_window: int = 5) -> pd.DataFrame:
    """
    Relaxed aggregation version:
    group events by:
    - src_user
    - dst_computer
    - event_type
    """
    working_df = df.copy()
    working_df = working_df.sort_values("timestamp").reset_index(drop=True)

    key_columns = [
        "src_user",
        "dst_computer",
        "event_type",
    ]

    aggregated_rows = []
    current_group = None
    group_id = 1

    for _, row in working_df.iterrows():
        row_key = tuple(row[col] for col in key_columns)
        row_time = row["timestamp"]

        if current_group is None:
            current_group = {
                "group_id": group_id,
                "start_time": row_time,
                "end_time": row_time,
                "event_count": 1,
                "src_user": row["src_user"],
                "dst_user": row["dst_user"],
                "src_computer": row["src_computer"],
                "dst_computer": row["dst_computer"],
                "event_type": row["event_type"],
                "auth_type": row["auth_type"],
                "logon_type": row["logon_type"],
                "result": row["result"],
                "key": row_key,
            }
            continue

        same_key = row_key == current_group["key"]
        within_window = (row_time - current_group["end_time"]) <= time_window

        if same_key and within_window:
            current_group["end_time"] = row_time
            current_group["event_count"] += 1
        else:
            aggregated_rows.append({
                "group_id": current_group["group_id"],
                "start_time": current_group["start_time"],
                "end_time": current_group["end_time"],
                "event_count": current_group["event_count"],
                "src_user": current_group["src_user"],
                "dst_user": current_group["dst_user"],
                "src_computer": current_group["src_computer"],
                "dst_computer": current_group["dst_computer"],
                "event_type": current_group["event_type"],
                "auth_type": current_group["auth_type"],
                "logon_type": current_group["logon_type"],
                "result": current_group["result"],
            })

            group_id += 1
            current_group = {
                "group_id": group_id,
                "start_time": row_time,
                "end_time": row_time,
                "event_count": 1,
                "src_user": row["src_user"],
                "dst_user": row["dst_user"],
                "src_computer": row["src_computer"],
                "dst_computer": row["dst_computer"],
                "event_type": row["event_type"],
                "auth_type": row["auth_type"],
                "logon_type": row["logon_type"],
                "result": row["result"],
                "key": row_key,
            }

    if current_group is not None:
        aggregated_rows.append({
            "group_id": current_group["group_id"],
            "start_time": current_group["start_time"],
            "end_time": current_group["end_time"],
            "event_count": current_group["event_count"],
            "src_user": current_group["src_user"],
            "dst_user": current_group["dst_user"],
            "src_computer": current_group["src_computer"],
            "dst_computer": current_group["dst_computer"],
            "event_type": current_group["event_type"],
            "auth_type": current_group["auth_type"],
            "logon_type": current_group["logon_type"],
            "result": current_group["result"],
        })

    aggregated_df = pd.DataFrame(aggregated_rows)
    return aggregated_df