"""Observe managed copy initiation and cleanup; botocore owns request retries."""

from typing import Any, Mapping

from ch_backup import logging
from ch_backup.exceptions import StorageError
from ch_backup.type_hints.boto3.s3 import S3Client


class S3MultipartCopyError(StorageError):
    """An initiated multipart copy failed; do not restart it or stream the object."""


class S3CopyClient:
    """Delegate to the SDK while tracking whether streaming fallback is safe."""

    def __init__(self, client: S3Client) -> None:
        self._client = client
        self.upload_id: str | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def create_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]:
        """Remember initiation so later failures cannot restart the whole object."""
        response = self._client.create_multipart_upload(**kwargs)
        self.upload_id = response["UploadId"]
        return response

    def abort_multipart_upload(self, **kwargs: Any) -> Mapping[str, Any]:
        """Make SDK cleanup observable without replacing the transfer error."""
        try:
            response = self._client.abort_multipart_upload(**kwargs)
        except Exception as error:
            logging.warning(
                "AbortMultipartUpload failed for s3://{}/{} (UploadId {}): {}",
                kwargs["Bucket"],
                kwargs["Key"],
                kwargs["UploadId"],
                error,
            )
            # s3transfer preserves the original error and suppresses cleanup errors.
            raise
        logging.info(
            "Aborted multipart copy for s3://{}/{} (UploadId {})",
            kwargs["Bucket"],
            kwargs["Key"],
            kwargs["UploadId"],
        )
        return response
