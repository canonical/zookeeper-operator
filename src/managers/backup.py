#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helpers for managing backups."""

import logging
from datetime import datetime

import boto3
import httpx
import yaml
from botocore import loaders, regions
from botocore.exceptions import ClientError
from mypy_boto3_s3.service_resource import Bucket

from core.stubs import BackupMetadata, S3ConnectionInfo
from literals import ADMIN_SERVER_PORT

logger = logging.getLogger(__name__)


class BackupManager:
    """Manager for all things backup-related."""

    def __init__(self, s3_parameters: S3ConnectionInfo) -> None:
        self.s3_parameters = s3_parameters

    @property
    def bucket(self) -> Bucket:
        """S3 bucket to read from and write to."""
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=self.s3_parameters["access-key"],
            aws_secret_access_key=self.s3_parameters["secret-key"],
            region_name=self.s3_parameters["region"] if self.s3_parameters["region"] else None,
            endpoint_url=self._construct_endpoint(self.s3_parameters),
        )
        return s3.Bucket(self.s3_parameters["bucket"])

    def _construct_endpoint(self, s3_parameters: S3ConnectionInfo) -> str:
        """Construct the S3 service endpoint using the region.

        This is needed when the provided endpoint is from AWS, and it doesn't contain the region.
        """
        endpoint = s3_parameters["endpoint"]

        loader = loaders.create_loader()
        data = loader.load_data("endpoints")

        resolver = regions.EndpointResolver(data)
        endpoint_data = resolver.construct_endpoint("s3", s3_parameters["region"])

        if endpoint_data and endpoint.endswith(endpoint_data["dnsSuffix"]):
            endpoint = f'{endpoint.split("://")[0]}://{endpoint_data["hostname"]}'

        return endpoint

    def create_bucket(self, s3_parameters: S3ConnectionInfo) -> bool:
        """Create bucket if it does not exists."""
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=s3_parameters["access-key"],
            aws_secret_access_key=s3_parameters["secret-key"],
            region_name=s3_parameters["region"] if s3_parameters["region"] else None,
            endpoint_url=s3_parameters["endpoint"],
        )
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

    def write_test_string(self) -> None:
        """Write content in the object storage."""
        self.bucket.put_object(Key="test_file.txt", Body=b"test string")

    def create_backup(self, zk_host: str, zk_user: str, zk_pwd: str):
        """Create a snapshot with ZooKeeper admin server and stream it to the object storage."""
        date = datetime.now()
        snapshot_name = f"{date:%Y-%m-%dT%H:%M:%SZ}"
        snapshot_path = f"{snapshot_name}/snapshot"

        # It is very likely that the file is loaded in memory, because the file-like interface is not yet seekable.
        # We cannot be sure because finding this information in boto code base is time consuming.
        # If this ever become an issue, we can find a workaround by using the 'content-length' header from the
        # admin server. Or write to a temp file.
        with httpx.stream(
            "GET",
            f"http://{zk_host}:{ADMIN_SERVER_PORT}/commands/snapshot?streaming=true",
            headers={"Authorization": f"digest {zk_user}:{zk_pwd}"},
        ) as response:

            response_headers = response.headers
            quorum_leader_zxid = int(response_headers["last_zxid"], base=16)
            metadata: BackupMetadata = {
                "id": snapshot_name,
                "log-sequence-number": quorum_leader_zxid,
                "path": snapshot_path,
            }

            self.bucket.upload_fileobj(_StreamingToFileSyncAdapter(response), f"{snapshot_name}/snapshot")  # type: ignore
            self.bucket.put_object(
                Key=f"{snapshot_name}/metadata.yaml", Body=yaml.dump(metadata, encoding="utf8")
            )

        return metadata

    def display_backups_metadata(self, backup_entries: list[BackupMetadata]):
        pass


class _StreamingToFileSyncAdapter:
    """Wrapper to make httpx.stream behave like a file-like object.

    boto needs a .read method with an optional amount-of-bytes parameter from the file-like object.
    Taken from https://github.com/encode/httpx/discussions/2296#discussioncomment-6781355
    """

    def __init__(self, response: httpx.Response):
        self.streaming_source = response.iter_bytes()
        self.buffer = b""
        self.buffer_offset = 0

    def read(self, num_bytes: int = 4096) -> bytes:
        while len(self.buffer) - self.buffer_offset < num_bytes:
            try:
                chunk = next(self.streaming_source)
                self.buffer += chunk
            except StopIteration:
                break

        if len(self.buffer) - self.buffer_offset >= num_bytes:
            data = self.buffer[self.buffer_offset : self.buffer_offset + num_bytes]
            self.buffer_offset += num_bytes
            return data
        else:
            data = self.buffer[self.buffer_offset :]
            self.buffer = b""
            self.buffer_offset = 0
            return data
