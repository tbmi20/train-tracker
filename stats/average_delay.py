"""Average departure delay (minutes) per train (uid) on stat_date.

Second example metric, proving the modular pattern from
on_time_percentage.py: same underlying observations, different
aggregation, no shared state between the two.
"""

import pandas as pd

from stats._util import delay_minutes

METRIC_NAME = "avg_delay_minutes"


def compute(db, stat_date: str) -> list[dict]:
    rows = db.get_departure_observations(stat_date)
    if not rows:
        return []

    df = pd.DataFrame(rows)
    df["observed_dep"] = df["act_dep"].fillna(df["est_dep"])
    df["delay"] = df.apply(
        lambda r: delay_minutes(r["planned_dep"], r["observed_dep"]), axis=1
    )
    df = df.dropna(subset=["delay"])
    if df.empty:
        return []

    avg_by_uid = df.groupby("uid")["delay"].mean()
    return [
        {"scope_type": "uid", "scope_value": uid, "value": round(avg, 1)}
        for uid, avg in avg_by_uid.items()
    ]
