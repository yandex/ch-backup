"""
S3 multipart uploader.
"""

import operator
import threading
import time
from typing import Dict, Optional

from ch_backup.storage.engine.s3.s3_client_factory import S3ClientCachedFactory
from ch_backup.type_hints.boto3.s3 import S3Client


class S3MultipartUploader:
    """
    Thread safe S3 multipart object uploader.
    """

    def __init__(
        self, bucket_name: str, s3_client_factory: S3ClientCachedFactory
    ) -> None:
        self._bucket_name = bucket_name
        self._s3_client_factory = s3_client_factory
        self._lock = threading.Lock()

        self._uploads: Dict[str, dict] = {}

    @property
    def _s3_client(self) -> S3Client:
        """
        Get S3 client.
        """
        return self._s3_client_factory.create_s3_client()

    def create_multipart_upload(self, remote_path: str) -> str:
        """
        Initiate multipart upload.
        """
        resp = self._s3_client.create_multipart_upload(
            Bucket=self._bucket_name, Key=remote_path
        )
        upload_id = resp["UploadId"]

        # TODO: limit multipart uploads + clean up expired
        with self._lock:
            self._uploads[upload_id] = {"ctime": int(time.time()), "Parts": []}

        return upload_id

    def upload_part(
        self,
        data: bytes,
        remote_path: str,
        upload_id: str,
        part_num: Optional[int] = None,
    ) -> None:
        """
        Upload part to S3 storage for specified multipart upload.
        """
        if part_num is None:
            with self._lock:
                upload_parts = self._uploads[upload_id]["Parts"]
                try:
                    part_num = upload_parts[-1]["PartNumber"] + 1
                except IndexError:
                    part_num = 1

        resp = self._s3_client.upload_part(
            Body=data,
            Bucket=self._bucket_name,
            Key=remote_path,
            UploadId=upload_id,
            PartNumber=part_num,
        )

        # save part metadata for complete upload
        with self._lock:
            self._uploads[upload_id]["Parts"].append(
                {"ETag": resp["ETag"], "PartNumber": part_num}
            )

    def complete_multipart_upload(self, remote_path: str, upload_id: str) -> None:
        """
        Complete multipart upload.
        """
        with self._lock:
            parts = self._uploads[upload_id]["Parts"]

        self._s3_client.complete_multipart_upload(
            Bucket=self._bucket_name,
            Key=remote_path,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": sorted(parts, key=operator.itemgetter("PartNumber"))
            },
            # The parts list must be specified in order of part numbers
        )

        with self._lock:
            self._uploads.pop(upload_id, None)
