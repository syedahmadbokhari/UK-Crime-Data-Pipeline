"""
Uploads local crime CSVs to S3 with Hive-style partitioning:
    s3://<bucket>/crime/year=YYYY/month=MM/force=<force>/<filename>.csv

Usage:
    python -m ingestion.upload_to_s3 --force west-yorkshire --month 2024-01
"""
import argparse
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from loguru import logger

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "")
S3_PREFIX = os.getenv("S3_PREFIX", "crime")
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "./data/raw"))


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-2"),
    )


def s3_key(force: str, year_month: str, filename: str) -> str:
    """Build the partitioned S3 key."""
    year, month = year_month.split("-")
    return f"{S3_PREFIX}/year={year}/month={month}/force={force}/{filename}"


def upload_file(local_path: Path, force: str, year_month: str) -> str | None:
    """
    Upload a single CSV to S3.
    Returns the S3 URI on success, None on failure.
    """
    # Read at call time so tests can override via monkeypatch/env
    bucket = os.getenv("S3_BUCKET_NAME", "")
    if not bucket:
        raise EnvironmentError("S3_BUCKET_NAME environment variable not set")

    key = s3_key(force, year_month, local_path.name)
    s3 = get_s3_client()

    # Check if already uploaded (idempotent)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Already in S3: s3://{bucket}/{key} — skipping")
        return f"s3://{bucket}/{key}"
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise

    logger.info(f"Uploading {local_path.name} → s3://{bucket}/{key}")
    try:
        s3.upload_file(
            str(local_path),
            bucket,
            key,
            ExtraArgs={"ContentType": "text/csv"},
        )
        uri = f"s3://{bucket}/{key}"
        logger.success(f"Uploaded: {uri}")
        return uri
    except ClientError as e:
        logger.error(f"Upload failed: {e}")
        return None


def upload_month(force: str, year_month: str) -> list[str]:
    """Upload all CSVs for a given force + month. Returns list of S3 URIs."""
    pattern = f"{year_month}-{force}-street.csv"
    files = list(RAW_DATA_DIR.glob(pattern))

    if not files:
        # Also accept files already named with the force in them
        files = list(RAW_DATA_DIR.glob(f"*{year_month}*{force}*street*.csv"))

    if not files:
        logger.warning(f"No local files found matching {pattern}")
        return []

    uris = []
    for f in files:
        uri = upload_file(f, force, year_month)
        if uri:
            uris.append(uri)
    return uris


def list_s3_partitions() -> list[dict]:
    """List all year/month/force partitions currently in S3."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    partitions = set()

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            parts = prefix["Prefix"].rstrip("/").split("/")
            if len(parts) >= 4:
                partitions.add(tuple(parts[1:4]))

    return [
        {"year": p[0], "month": p[1], "force": p[2]}
        for p in sorted(partitions)
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload crime CSVs to S3")
    parser.add_argument("--force", required=True)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()

    uris = upload_month(args.force, args.month)
    logger.info(f"Uploaded {len(uris)} file(s)")
