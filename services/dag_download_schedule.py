"""
Nightly CIF schedule download DAG.

Runs at 02:00, retries on failure with backoff, gives up by ~05:00 to
leave the rest of the ELT pipeline room to run before the morning
commute window. Uses the TaskFlow API (Airflow 2.x+).
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from s3_downloader import (
    S3Config,
    cleanup_old_versions,
    download_file,
    get_s3_client,
    list_available_files,
    unzip_file,
)

DOWNLOAD_DIR = Path("/opt/train_tracker/downloads")
EXTRACT_DIR = Path("/opt/train_tracker/extracted")
RETAIN_VERSIONS = 2

# 6 retries at 30 min apart = 3 hours of retry window after the initial
# attempt at 02:00, so the task gives up by ~05:00 as intended. Tune
# retry_delay/retries together if you change the start time.
default_args = {
    "retries": 6,
    "retry_delay": timedelta(minutes=30),
}


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
    def list_new_files() -> list[dict]:
        config = S3Config.from_env()
        client = get_s3_client(config.region)
        all_files = list_available_files(client, config)

        # DECISION POINT — plug this into your manifest-based dedup
        # check (the same pattern your incremental ETL bullet already
        # describes): query the manifest table for keys already
        # processed and filter them out here, rather than
        # re-downloading/re-extracting files you've already ingested.
        # Placeholder below just returns everything found.
        new_files = all_files
        return new_files

    @task
    def download_and_extract(files: list[dict]) -> list[str]:
        config = S3Config.from_env()
        client = get_s3_client(config.region)
        extracted_paths: list[str] = []

        for f in files:
            key = f["key"]
            local_archive = DOWNLOAD_DIR / Path(key).name
            download_file(client, config.bucket, key, local_archive)
            extracted = unzip_file(local_archive, EXTRACT_DIR)
            extracted_paths.extend(str(p) for p in extracted)

        return extracted_paths

    @task
    def cleanup_old():
        deleted = cleanup_old_versions(EXTRACT_DIR, keep=RETAIN_VERSIONS)
        return [str(p) for p in deleted]

    files = list_new_files()
    extracted = download_and_extract(files)
    # cleanup runs after extraction of the new version succeeds, so we
    # never delete the old version before the new one is safely down
    cleanup_old().set_upstream(extracted)


nightly_schedule_download()
