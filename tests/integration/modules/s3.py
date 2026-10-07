"""
S3 client.
"""

import logging
import os

import boto3
from botocore.client import Config
from botocore.errorfactory import ClientError
from botocore.exceptions import BotoCoreError
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

from . import docker
from .typing import ContextT


class S3NotReadyError(Exception):
    """
    S3 is not ready to serve requests yet.
    """


class S3Client:
    """
    S3 client.
    """

    def __init__(self, context: ContextT, bucket: str | None = None) -> None:
        config = context.conf["s3"]
        boto_config = config["boto_config"]
        self._s3_session = boto3.session.Session(
            aws_access_key_id=config["access_key_id"],
            aws_secret_access_key=config["access_secret_key"],
            region_name=boto_config["region_name"],
        )

        host, port = docker.get_exposed_port(
            docker.get_container(context, context.conf["s3"]["container"]),
            context.conf["s3"]["port"],
        )
        endpoint_url = f"http://{host}:{port}"
        self._s3_client = self._s3_session.client(
            service_name="s3",
            endpoint_url=endpoint_url,
            config=Config(
                s3={
                    "addressing_style": boto_config["addressing_style"],
                    "region_name": boto_config["region_name"],
                }
            ),
        )

        if bucket:
            self._s3_bucket_name = bucket
        else:
            self._s3_bucket_name = config["bucket"]

        for module_logger in ("boto3", "botocore", "s3transfer", "urllib3"):
            logging.getLogger(module_logger).setLevel(logging.CRITICAL)

    def upload_data(self, data: bytes, remote_path: str) -> None:
        """
        Upload given bytes or file-like object.
        """
        remote_path = remote_path.lstrip("/")
        self._s3_client.put_object(
            Body=data, Bucket=self._s3_bucket_name, Key=remote_path
        )

    def download_file(self, remote_path: str, local_path: str) -> None:
        """
        Download file from storage to the local path.
        """
        self._s3_client.download_file(self._s3_bucket_name, remote_path, local_path)

    def delete_data(self, remote_path: str) -> None:
        """
        Delete file from storage.
        """
        remote_path = remote_path.lstrip("/")
        self._s3_client.delete_object(Bucket=self._s3_bucket_name, Key=remote_path)

    def path_exists(self, remote_path: str) -> bool:
        """
        Check if remote path exists.
        """
        try:
            self._s3_client.head_object(Bucket=self._s3_bucket_name, Key=remote_path)
            return True
        except ClientError:
            return False

    def get_object_size(self, remote_path: str) -> int:
        """
        Return size of the object in bytes.
        """
        response = self._s3_client.head_object(
            Bucket=self._s3_bucket_name, Key=remote_path
        )
        return response["ContentLength"]

    def bucket_exists(self) -> bool:
        """
        Check if the bucket exists.
        """
        try:
            self._s3_client.head_bucket(Bucket=self._s3_bucket_name)
            return True
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            raise

    def list_objects(self, prefix: str) -> list[str]:
        """
        List all objects with given prefix.
        """
        contents = []
        paginator = self._s3_client.get_paginator("list_objects")
        list_object_kwargs = dict(Bucket=self._s3_bucket_name, Prefix=prefix)

        for result in paginator.paginate(**list_object_kwargs):
            if result.get("CommonPrefixes") is not None:
                for dir_prefix in result.get("CommonPrefixes"):
                    contents.append(dir_prefix.get("Prefix"))

            if result.get("Contents") is not None:
                for file_key in result.get("Contents"):
                    contents.append(file_key.get("Key"))

        return contents


@retry(
    retry=retry_if_exception_type((S3NotReadyError, BotoCoreError)),
    wait=wait_fixed(0.5),
    stop=stop_after_delay(180),
    reraise=True,
)
def wait_for_s3_buckets(context: ContextT) -> None:
    """
    Wait until all S3 buckets specified in the config exist.
    """
    for bucket in context.conf["s3"]["buckets"]:
        if not S3Client(context, bucket).bucket_exists():
            raise S3NotReadyError(f"S3 bucket {bucket} does not exist")


def export_s3_data(context: ContextT, path: str) -> None:
    """
    Export S3 data to the specified directory.
    """
    for bucket in context.conf["s3"]["buckets"]:
        try:
            s3_client = S3Client(context, bucket)
            keys = s3_client.list_objects("")
        except Exception:
            logging.exception("Failed to list S3 bucket %s", bucket)
            continue

        for key in keys:
            if key.endswith("/"):
                continue
            local_path = os.path.join(path, "s3", bucket, key.lstrip("/"))
            try:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                s3_client.download_file(key, local_path)
            except Exception:
                logging.exception("Failed to export S3 object %s/%s", bucket, key)
