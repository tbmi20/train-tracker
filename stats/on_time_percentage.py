"""On-time percentage per train (uid): the share of a train's observed
departures on stat_date within ON_TIME_THRESHOLD_MINUTES of scheduled.

This is the metric services/database.py's live-status queries
(get_upcoming_departures, get_train_status) read as the displayed
reliability figure - see RELIABILITY_METRIC in that file.
"""

import pandas as pd

from services.database import RELIABILITY_METRIC
from stats._util import delay_minutes

METRIC_NAME = RELIABILITY_METRIC
ON_TIME_THRESHOLD_MINUTES = 5


def compute(db, stat_date: str) -> list[dict]:
    rows = db.get_departure_observations(stat_date)
    if not rows:
        return []

    df = pd.DataFrame(rows)
    # Actual departure time if we have it; otherwise the last estimate -
    # still counts as "observed" even if Darwin never confirmed the actual.
    df["observed_dep"] = df["act_dep"].fillna(df["est_dep"])
    df["delay"] = df.apply(
        lambda r: delay_minutes(r["planned_dep"], r["observed_dep"]), axis=1
    )
    df = df.dropna(subset=["delay"])
    if df.empty:
        return []

    df["on_time"] = df["delay"].abs() <= ON_TIME_THRESHOLD_MINUTES
    pct_by_uid = df.groupby("uid")["on_time"].mean() * 100

    return [
        {"scope_type": "uid", "scope_value": uid, "value": round(pct, 1)}
        for uid, pct in pct_by_uid.items()
    ]
