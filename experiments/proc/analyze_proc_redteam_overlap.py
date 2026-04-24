from pathlib import Path
import pandas as pd

proc_path = Path("data/raw/proc.txt")
redteam_path = Path("data/raw/redteam.txt")

print("PROC exists:", proc_path.exists())
print("REDTEAM exists:", redteam_path.exists())

if proc_path.exists() and redteam_path.exists():
    proc_df = pd.read_csv(
        proc_path,
        header=None,
        nrows=10000,
        names=[
            "timestamp",
            "user",
            "computer",
            "process",
            "event_state"
        ]
    )

    redteam_df = pd.read_csv(
        redteam_path,
        header=None,
        names=[
            "timestamp",
            "user",
            "src_computer",
            "dst_computer"
        ]
    )

    print("\n" + "=" * 60)
    print("TIME OVERLAP")
    print("=" * 60)
    print("PROC range:", proc_df["timestamp"].min(), "->", proc_df["timestamp"].max())
    print("REDTEAM range:", redteam_df["timestamp"].min(), "->", redteam_df["timestamp"].max())

    proc_in_redteam_range = proc_df[
        (proc_df["timestamp"] >= redteam_df["timestamp"].min()) &
        (proc_df["timestamp"] <= redteam_df["timestamp"].max())
    ]

    print("PROC rows inside redteam time range:", len(proc_in_redteam_range))

    print("\n" + "=" * 60)
    print("USER OVERLAP")
    print("=" * 60)

    proc_users = set(proc_df["user"].astype(str).unique())
    redteam_users = set(redteam_df["user"].astype(str).unique())
    common_users = sorted(proc_users & redteam_users)

    print("Unique proc users:", len(proc_users))
    print("Unique redteam users:", len(redteam_users))
    print("Common users:", len(common_users))
    print("Example common users:", common_users[:20])

    print("\n" + "=" * 60)
    print("COMPUTER OVERLAP")
    print("=" * 60)

    proc_computers = set(proc_df["computer"].astype(str).unique())
    redteam_src = set(redteam_df["src_computer"].astype(str).unique())
    redteam_dst = set(redteam_df["dst_computer"].astype(str).unique())
    redteam_all = redteam_src | redteam_dst

    common_computers = sorted(proc_computers & redteam_all)

    print("Unique proc computers:", len(proc_computers))
    print("Unique redteam computers:", len(redteam_all))
    print("Common computers:", len(common_computers))
    print("Example common computers:", common_computers[:20])
else:
    print("One or both files are missing.")