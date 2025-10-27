#!/usr/bin/env python3
"""
S3 Integration — src/cloud/s3_handler.py

Uploads raw CSVs to S3 in a dated layout:
  s3://<bucket>/<prefix>/year=YYYY/month=MM/day=DD/<dataset>/<file>.csv

Features
- Optional: enable bucket versioning
- Retries with exponential backoff (plus boto3 built-ins)
- Server-side encryption (SSE-S3)
- Configurable datasets and source folder

Examples
  python src/cloud/s3_handler.py --bucket ctw04531-shopflow-bucket --enable-versioning
  python src/cloud/s3_handler.py --bucket ctw04531-shopflow-bucket --date 2025-01-15
  python src/cloud/s3_handler.py --bucket <bucket> --base-path data/raw --datasets customers products transactions
"""

from __future__ import annotations
import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

# Defaults
DEFAULT_DATASETS = ["customers", "products", "transactions"]
DEFAULT_PREFIX   = "raw"
DEFAULT_BASEPATH = "data/raw"
DEFAULT_REGION   = os.getenv("AWS_REGION")  # optional

# boto3 retry & transfer tuning
BOTO_CFG = Config(
    retries={"max_attempts": 8, "mode": "standard"},
    region_name=DEFAULT_REGION
)
XFER_CFG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,  # 8MB
    multipart_chunksize=8 * 1024 * 1024,  # 8MB
    max_concurrency=8,
    use_threads=True,
)

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upload CSVs to S3 with date partitions + retries.")
    p.add_argument("--bucket", required=True, help="Target S3 bucket name")
    p.add_argument("--prefix", default=DEFAULT_PREFIX, help="Top-level S3 prefix (default: raw)")
    p.add_argument("--base-path", default=DEFAULT_BASEPATH, help="Local folder containing CSVs (default: data/raw)")
    p.add_argument("--datasets", nargs="+", default=DEFAULT_DATASETS,
                   help="Datasets to upload (file stems). Default: customers products transactions")
    p.add_argument("--date", help="Partition date YYYY-MM-DD (default: today UTC)")
    p.add_argument("--enable-versioning", action="store_true", help="Enable bucket versioning before upload")
    return p.parse_args()

def parse_date(d: str | None) -> dt.date:
    return dt.datetime.strptime(d, "%Y-%m-%d").date() if d else dt.datetime.utcnow().date()

def ensure_versioning(bucket: str, s3_client):
    status = s3_client.get_bucket_versioning(Bucket=bucket).get("Status")
    if status != "Enabled":
        s3_client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        print(f"Versioning enabled on bucket: {bucket}")
    else:
        print(f"Versioning already enabled on bucket: {bucket}")

def backoff_upload(s3, bucket: str, src: Path, key: str, max_retries: int = 5):
    attempt = 0
    while True:
        attempt += 1
        try:
            s3.upload_file(
                Filename=str(src),
                Bucket=bucket,
                Key=key,
                ExtraArgs={"ServerSideEncryption": "AES256"},
                Config=XFER_CFG,
            )
            print(f"OK  {src} -> s3://{bucket}/{key}")
            return
        except Exception as e:
            if attempt >= max_retries:
                print(f"FAIL {src} -> {key} after {attempt} attempts: {e}", file=sys.stderr)
                raise
            sleep = min(2 ** attempt, 30)  # 2,4,8,16,30
            print(f"RETRY {attempt} for {src}: {e} (sleep {sleep}s)", file=sys.stderr)
            time.sleep(sleep)

def build_key(prefix: str, when: dt.date, dataset: str, filename: str) -> str:
    y, m, d = when.strftime("%Y"), when.strftime("%m"), when.strftime("%d")
    return f"{prefix}/year={y}/month={m}/day={d}/{dataset}/{filename}"

def find_local_files(base_path: Path, datasets: List[str]) -> Dict[str, Path]:
    files: Dict[str, Path] = {}
    for ds in datasets:
        candidate = base_path / f"{ds}.csv"
        if not candidate.exists():
            raise FileNotFoundError(f"Missing file: {candidate}")
        files[ds] = candidate
    return files

def main():
    args     = parse_args()
    bucket   = args.bucket
    prefix   = args.prefix.rstrip("/")
    basepath = Path(args.base_path)
    when     = parse_date(args.date)

    files = find_local_files(basepath, args.datasets)
    s3 = boto3.client("s3", config=BOTO_CFG)

    if args.enable_versioning:
        ensure_versioning(bucket, s3)

    for ds, local_path in files.items():
        key = build_key(prefix, when, ds, local_path.name)
        backoff_upload(s3, bucket, local_path, key)

    print("\nUpload manifest:")
    for ds, local_path in files.items():
        key = build_key(prefix, when, ds, local_path.name)
        print(f"- {ds}: s3://{bucket}/{key}")

if __name__ == "__main__":
    main()
