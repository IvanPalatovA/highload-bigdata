#!/usr/bin/env python3
"""Delete whole S3 partitions that exceeded the retention period."""
import argparse
import os
import re
from datetime import date, timedelta

import boto3
from botocore.config import Config


def client():
    return boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name=os.environ["S3_REGION"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"], aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    )


def partition_date(key: str):
    match = re.search(r"(?:^|/)(?:dt|event_date)=(\d{4}-\d{2}-\d{2})(?:/|$)", key)
    return date.fromisoformat(match.group(1)) if match else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", choices=("raw", "clean"), required=True)
    parser.add_argument("--days", type=int, required=True, help="retain this many UTC days")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.days < 0:
        parser.error("--days must be non-negative")

    prefix = f"{args.layer}/"
    cutoff = date.today() - timedelta(days=args.days)
    s3, bucket = client(), os.environ.get("S3_BUCKET", "lake")
    paginator = s3.get_paginator("list_objects_v2")
    stale = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            part = partition_date(item["Key"])
            if part is not None and part < cutoff:
                stale.append(item["Key"])

    action = "would delete" if args.dry_run else "delete"
    for key in stale:
        print(f"{action} s3://{bucket}/{key}")
    if not args.dry_run:
        for start in range(0, len(stale), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in stale[start:start + 1000]]})
    print(f"{action}: {len(stale)} objects older than {cutoff.isoformat()}")


if __name__ == "__main__":
    main()

