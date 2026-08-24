"""
Nightly reliability stats DAG.

Computes every registered metric under stats/ (see stats/__init__.py for
the modular pattern - adding a metric is "add a file there", no DAG
changes) for the previous day's observed live data, and persists results
into daily_stats.

No hard dependency on nightly_schedule_download: this reads live_journeys/
live_journey_events, which the always-on ingest service (main.py) writes
continuously regardless of when the schedule was last refreshed. Scheduled
a little after that DAG mostly to keep the two nightly batch jobs out of
each other's way, not because one needs the other to finish first.
"""

from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="nightly_stats",
    schedule="15 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["train_tracker", "stats"],
)
def nightly_stats():

    @task
    def compute_stats() -> dict:
        import os

        import stats
        from services.database import Database

        # Simplification: "yesterday" in UTC, not the UK-local traffic day
        # (which would matter for a handful of trains near midnight during
        # BST) - consistent with the other provisional date/time handling
        # already noted in services/models.py (TrainUpdate.ssd).
        stat_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )

        database = Database(os.environ["DB_PATH"])
        database.initialise_schema()
        counts = stats.run_all(database, stat_date)
        database.close()
        return counts

    compute_stats()


nightly_stats()
