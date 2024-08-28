#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers for managing backups."""

import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class BackupManager:
    """Manager for all things backup-related."""

    def __init__(self) -> None:
        pass

    def create_bucket(self, s3_parameters: dict) -> bool:
        """Create bucket if it does not exists."""
        session = boto3.Session(
            aws_access_key_id=s3_parameters["access-key"],
            aws_secret_access_key=s3_parameters["secret-key"],
            region_name=s3_parameters["region"] if s3_parameters["region"] else None,
        )

        s3 = session.resource("s3")
        bucket_name = s3_parameters["bucket"]
        bucket_exists = True

        bucket = s3.Bucket(bucket_name)  # pyright: ignore [reportAttributeAccessIssue]

        try:
            bucket.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError as ex:
            if "(403)" in ex.args[0]:
                logger.error("Wrong credentials or access to bucket is forbidden")
                return False
            elif "(404)" in ex.args[0]:
                bucket_exists = False
        else:
            logger.info(f"Using existing bucket {bucket_name}")

        if not bucket_exists:
            bucket.create()
            bucket.wait_until_exists()
            logger.info(f"Created bucket {bucket_name}")

        return True

    def write_test_string(self, s3_parameters: dict) -> None:
        """Write content in the object storage."""
        session = boto3.Session(
            aws_access_key_id=s3_parameters["access-key"],
            aws_secret_access_key=s3_parameters["secret-key"],
            region_name=s3_parameters["region"] if s3_parameters["region"] else None,
        )

        s3 = session.resource("s3")
        bucket = s3.Bucket(s3_parameters["bucket"])  # pyright: ignore [reportAttributeAccessIssue]

        file = bucket.Object("test_file.txt")
        file.put(Body=b"somecontent")
