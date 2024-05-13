"""
S3 storage engine.
"""

import os
import time
from tempfile import TemporaryFile
from typing import Optional, Sequence

import requests
from botocore.exceptions import ClientError

from ch_backup.storage.engine.base import PipeLineCompatibleStorageEngine
from ch_backup.storage.engine.s3.s3_client_factory import (
    S3ClientCachedFactory,
    S3ClientFactory,
)
from ch_backup.storage.engine.s3.s3_multipart_uploader import S3MultipartUploader
from ch_backup.storage.engine.s3.s3_retry import S3RetryMeta
from ch_backup.type_hints.boto3.s3 import S3Client


class S3StorageEngine(PipeLineCompatibleStorageEngine, metaclass=S3RetryMeta):
    """
    Engine for S3-compatible storage services.
    """

    DEFAULT_DOWNLOAD_PART_LEN = 128 * 1024 * 1024

    def __init__(self, config: dict) -> None:
        self._s3_client_factory = S3ClientCachedFactory(S3ClientFactory(config))
        self._s3_bucket_name = config["credentials"]["bucket"]

        self._multipart_uploader = S3MultipartUploader(
            self._s3_bucket_name, self._s3_client_factory
        )
        self._multipart_downloads: dict = {}

        if config.get("disable_ssl_warnings"):
            requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

        self._bulk_delete_enabled = config.get("bulk_delete_enabled", True)

    @property
    def _s3_client(self) -> S3Client:
        return self._s3_client_factory.create_s3_client()

    def upload_file(self, local_path: str, remote_path: str) -> str:
        remote_path = remote_path.lstrip("/")
        with open(local_path, "rb") as data:
            self._s3_client.upload_fileobj(data, self._s3_bucket_name, remote_path)
        return remote_path

    def upload_data(self, data: bytes, remote_path: str) -> str:
        remote_path = remote_path.lstrip("/")
        self._s3_client.put_object(
            Body=data, Bucket=self._s3_bucket_name, Key=remote_path
        )
        return remote_path

    def download_file(self, remote_path: str, local_path: str) -> None:
        remote_path = remote_path.lstrip("/")
        self._s3_client.download_file(self._s3_bucket_name, remote_path, local_path)

    def download_data(self, remote_path):
        remote_path = remote_path.lstrip("/")
        with TemporaryFile() as fileobj:
            self._s3_client.download_fileobj(self._s3_bucket_name, remote_path, fileobj)
            fileobj.seek(0)
            data = fileobj.read()
        return data

    def delete_file(self, remote_path: str) -> None:
        remote_path = remote_path.lstrip("/")
        try:
            self._s3_client.delete_object(Bucket=self._s3_bucket_name, Key=remote_path)
        except ClientError as e:
            # delete_object should return success if no such object
            # https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
            # but GCP returns error in this case
            # https://cloud.google.com/storage/docs/xml-api/delete-object
            if e.response["Error"]["Code"] == "NoSuchKey":
                return
            raise

    def delete_files(self, remote_paths: Sequence[str]) -> None:
        """
        Delete multiple files from S3
        """

        def delete_by_one(remote_paths: Sequence[str]) -> None:
            for remote_path in remote_paths:
                self.delete_file(remote_path)

        if not self._bulk_delete_enabled:
            delete_by_one(remote_paths)
            return

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
        try:
            objects_to_delete: list = [
                {"Key": path.lstrip("/")} for path in remote_paths
            ]
            self._s3_client.delete_objects(
                Bucket=self._s3_bucket_name, Delete={"Objects": objects_to_delete}
            )
        except ClientError as e:
            if "MalformedXML" not in repr(e):
                raise
            delete_by_one(remote_paths)

    def list_dir(
        self, remote_path: str, recursive: bool = False, absolute: bool = False
    ) -> Sequence[str]:
        remote_path = remote_path.strip("/") + "/"
        contents = []
        paginator = self._s3_client.get_paginator("list_objects")
        list_object_kwargs = dict(Bucket=self._s3_bucket_name, Prefix=remote_path)
        if not recursive:
            list_object_kwargs["Delimiter"] = "/"

        for page in paginator.paginate(**list_object_kwargs):
            common_prefixes = page.get("CommonPrefixes")
            if common_prefixes is not None:
                for dir_prefix in common_prefixes:
                    if absolute:
                        dir_path = dir_prefix["Prefix"]
                    else:
                        dir_path = os.path.relpath(dir_prefix["Prefix"], remote_path)

                    contents.append(dir_path)

            page_contents = page.get("Contents")
            if page_contents is not None:
                for file_key in page_contents:
                    if absolute:
                        file_path = file_key["Key"]
                    else:
                        file_path = os.path.relpath(file_key["Key"], remote_path)

                    contents.append(file_path)

        return contents

    def path_exists(self, remote_path: str) -> bool:
        """
        Check if remote path exists.
        """
        try:
            self._s3_client.head_object(Bucket=self._s3_bucket_name, Key=remote_path)
            return True
        except ClientError as ce:
            code = ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code == 404:
                return False
            raise ce

    def create_multipart_upload(self, remote_path: str) -> str:
        return self._multipart_uploader.create_multipart_upload(remote_path)

    def upload_part(
        self,
        data: bytes,
        remote_path: str,
        upload_id: str,
        part_num: Optional[int] = None,
    ) -> None:
        self._multipart_uploader.upload_part(
            data, remote_path, upload_id, part_num=part_num
        )

    def complete_multipart_upload(self, remote_path: str, upload_id: str) -> None:
        self._multipart_uploader.complete_multipart_upload(remote_path, upload_id)

    def create_multipart_download(self, remote_path: str) -> str:
        remote_path = remote_path.lstrip("/")

        resp = self._s3_client.get_object(Bucket=self._s3_bucket_name, Key=remote_path)
        download_id = f"{remote_path}_{time.time()}"
        self._multipart_downloads[download_id] = {
            "path": remote_path,
            "range_start": 0,
            "total_size": resp["ContentLength"],
        }

        return download_id

    def download_part(self, download_id: str, part_len: int = None) -> Optional[bytes]:
        if part_len:
            part_len = self.DEFAULT_DOWNLOAD_PART_LEN

        download = self._multipart_downloads[download_id]

        if download["range_start"] == download["total_size"]:
            return None

        range_end = min(download["range_start"] + part_len, download["total_size"])
        part = self._s3_client.get_object(
            Bucket=self._s3_bucket_name,
            Key=download["path"],
            Range=f'bytes={download["range_start"]}-{range_end - 1}',
        )
        buffer = part["Body"].read()
        download["range_start"] = range_end
        return buffer

    def complete_multipart_download(self, download_id):
        del self._multipart_downloads[download_id]

    def get_object_size(self, remote_path: str) -> int:
        """
        Returns remote object size in bytes.
        """
        return self._s3_client.get_object(Bucket=self._s3_bucket_name, Key=remote_path)[
            "ContentLength"
        ]

    def get_client(self):
        """
        Return S3 raw client.
        """
        return self._s3_client
