"""
S3 download + extraction helpers for the nightly CIF schedule pull.

Design note: this module only knows how to talk to S3 and the local
filesystem. It deliberately does NOT know about Postgres, "what counts as
new", or retention policy beyond a simple version count — those are
pipeline decisions that belong in the DAG / manifest table, not here.
Keeping this module dumb means you can unit test it without a database
or Airflow running.
"""

import gzip
import logging
import os
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.client import BaseClient
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    bucket: str
    prefix: str
    region: str

    @classmethod
    def from_env(cls) -> "S3Config":
        bucket = os.environ["S3_BUCKET_NAME"]
        prefix = os.environ.get("S3_SCHEDULE_PREFIX", "")
        region = os.environ.get("AWS_REGION", "eu-west-2")
        return cls(bucket=bucket, prefix=prefix, region=region)


def get_s3_client(region: str | None = None) -> BaseClient:
    """
    Credentials are picked up from env vars (AWS_ACCESS_KEY_ID /
    AWS_SECRET_ACCESS_KEY) automatically by boto3 — we don't pass them
    explicitly so the same code works if you later switch to an IAM
    role or Airflow Connection instead of raw env vars.
    """
    return boto3.client("s3", region_name=region or os.environ.get("AWS_REGION"))


def list_available_files(s3_client: BaseClient, config: S3Config) -> list[dict]:
    """
    Returns [{"key": ..., "last_modified": ..., "size": ...}, ...] for
    everything under the configured prefix. Whether a given file is
    "new" relative to what you've already processed is a decision for
    the caller (e.g. check against a manifest table) — this function
    just reports what's currently in the bucket.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=config.bucket, Prefix=config.prefix):
        for obj in page.get("Contents", []):
            files.append(
                {
                    "key": obj["Key"],
                    "last_modified": obj["LastModified"],
                    "size": obj["Size"],
                }
            )
    logger.info("Found %d files under s3://%s/%s", len(files), config.bucket, config.prefix)
    return files


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=4, max=60))
def download_file(s3_client: BaseClient, bucket: str, key: str, local_path: Path) -> Path:
    """
    Retries with exponential backoff (4s, 8s, 16s, 32s, 60s — 5 attempts).
    This is what gives you the "retry until 5am" behaviour when combined
    with Airflow's own task-level retries in the DAG — this handles
    transient network blips within a single task run, Airflow handles
    re-running the whole task if it fails outright.
    """
    local_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading s3://%s/%s -> %s", bucket, key, local_path)
    s3_client.download_file(bucket, key, str(local_path))
    return local_path


def unzip_file(archive_path: Path, extract_dir: Path) -> list[Path]:
    """
    Handles both .gz (single file, common for CIF full/update exports)
    and .zip (in case the feed you're pulling from bundles multiple
    files together). Returns the list of extracted file paths.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []

    if archive_path.suffix == ".gz":
        out_path = extract_dir / archive_path.stem  # strips the .gz
        with gzip.open(archive_path, "rb") as f_in, open(out_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        extracted.append(out_path)

    elif archive_path.suffix == ".zip":
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extract_dir)
            extracted.extend(extract_dir / name for name in zf.namelist())

    else:
        raise ValueError(f"Unsupported archive type: {archive_path.suffix}")

    logger.info("Extracted %d file(s) from %s", len(extracted), archive_path.name)
    return extracted


def cleanup_old_versions(extract_dir: Path, keep: int, pattern: str = "*") -> list[Path]:
    """
    Keeps the `keep` most recently modified files matching `pattern`
    in extract_dir and deletes the rest. This is what frees disk space
    before the ELT pipeline runs, per your CV bullet.
    """
    files = sorted(extract_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    to_delete = files[keep:]
    for f in to_delete:
        logger.info("Deleting old version: %s", f)
        f.unlink()
    return to_delete
