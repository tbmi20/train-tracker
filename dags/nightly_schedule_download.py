"""
Nightly CIF schedule download + upsert DAG.

Runs at 02:00, retries on failure with backoff, gives up by ~05:00 to leave
the rest of the day's pipeline room to run before the morning commute
window. Uses the TaskFlow API (Airflow 2.x+/3.x).

Two S3 objects are handled differently:
- S3_DAILY_UPDATE: a small incremental CIF delta. Cheap, so it's downloaded
  and processed every run.
- S3_WEEKLY_SCHEDULE: the full national CIF extract (tens of MB). Only
  actually changes ~weekly, so it's skipped unless S3's LastModified has
  moved past a local marker file - reprocessing the whole thing nightly
  would mean tens of thousands of redundant upserts for no reason.

Both funnel through the same load_timetable() parser - CIF's own
transaction_type (N/R/D) already makes upsert_schedule() safe to run
against either a full extract or an incremental delta.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from services.database import Database
from services.load_data import load_timetable
from services.s3_downloader import (
    S3Config,
    cleanup_old_versions,
    download_file,
    get_last_modified,
    get_s3_client,
    unzip_file,
)

DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "/opt/airflow/downloads"))
EXTRACT_DIR = Path(os.environ.get("EXTRACT_DIR", "/opt/airflow/extracted"))
RETAIN_VERSIONS = int(os.environ.get("RETAIN_VERSIONS", "2"))

# Where we remember the weekly file's last-processed S3 LastModified, to
# avoid reprocessing a ~tens-of-MB extract every night when it's unchanged.
WEEKLY_MARKER_PATH = EXTRACT_DIR / ".weekly_schedule_last_modified"

# 6 retries at 30 min apart = 3 hours of retry window after the initial
# attempt at 02:00, so the task gives up by ~05:00 as intended. Tune
# retry_delay/retries together if you change the start time.
default_args = {
    "retries": 6,
    "retry_delay": timedelta(minutes=30),
}


def _extract_mca_file(extracted: list[Path]) -> Path | None:
    """Picks the CIF main schedule file (HD/TI/BS/LO/LI/LT records) out of a
    set of extracted files. The full weekly extract unzips into several
    reference files (MSN, ALF, ZTR, ...) alongside the one that's actually
    in our parser's format - identified by name, not content, since more
    than one of those files can start with an 'HD' record.
    """
    for path in extracted:
        if "MCA" in path.stem.upper():
            return path
    # Daily update archives typically contain just the one delta file.
    if len(extracted) == 1:
        return extracted[0]
    return None


@dag(
    dag_id="nightly_schedule_download",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["train_tracker", "ingestion"],
)
def nightly_schedule_download():

    @task
    def download_daily_update() -> str | None:
        import os

        config = S3Config.from_env()
        client = get_s3_client(config.region)
        key = os.environ["S3_DAILY_UPDATE"]

        local_archive = DOWNLOAD_DIR / Path(key).name
        download_file(client, config.bucket, key, local_archive)
        extracted = unzip_file(local_archive, EXTRACT_DIR / "daily")
        mca_file = _extract_mca_file(extracted)
        return str(mca_file) if mca_file else None

    @task
    def download_weekly_schedule() -> str | None:
        import os

        config = S3Config.from_env()
        client = get_s3_client(config.region)
        key = os.environ["S3_WEEKLY_SCHEDULE"]

        last_modified = get_last_modified(client, config.bucket, key)
        if WEEKLY_MARKER_PATH.exists():
            previous = WEEKLY_MARKER_PATH.read_text().strip()
            if previous == last_modified.isoformat():
                return None  # unchanged since we last processed it

        local_archive = DOWNLOAD_DIR / Path(key).name
        download_file(client, config.bucket, key, local_archive)
        extracted = unzip_file(local_archive, EXTRACT_DIR / "weekly")
        mca_file = _extract_mca_file(extracted)

        WEEKLY_MARKER_PATH.parent.mkdir(parents=True, exist_ok=True)
        WEEKLY_MARKER_PATH.write_text(last_modified.isoformat())
        return str(mca_file) if mca_file else None

    @task
    def upsert_into_db(weekly_path: str | None, daily_path: str | None) -> dict:
        import os

        database = Database(os.environ["DB_PATH"])
        database.initialise_schema()

        counts = {}
        # Weekly first if present - it's the base schedule the daily delta
        # (N/R/D transactions) is meant to apply on top of.
        if weekly_path:
            counts["weekly"] = load_timetable(weekly_path, database)
        if daily_path:
            counts["daily"] = load_timetable(daily_path, database)

        database.close()
        return counts

    @task
    def cleanup_old():
        deleted = cleanup_old_versions(EXTRACT_DIR / "weekly", keep=RETAIN_VERSIONS)
        deleted += cleanup_old_versions(EXTRACT_DIR / "daily", keep=RETAIN_VERSIONS)
        return [str(p) for p in deleted]

    weekly = download_weekly_schedule()
    daily = download_daily_update()
    upserted = upsert_into_db(weekly, daily)
    # cleanup runs after the upsert succeeds, so we never delete a version
    # before it's been safely written to the DB
    cleanup_old().set_upstream(upserted)


nightly_schedule_download()
