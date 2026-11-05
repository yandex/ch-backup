"""Request adapter for managed server-side copy, including failure cleanup."""

from typing import Any, Callable

from ch_backup import logging
from ch_backup.exceptions import StorageError


class S3MultipartCopyError(StorageError):
    """An initiated multipart copy failed; do not restart it or stream the object."""


class S3CopyClient:
    """Let s3transfer manage parts while ch-backup retries individual requests."""

    def __init__(self, meta: Any, request: Callable[..., dict]) -> None:
        self.meta = meta
        self._request = request
        self.upload_id: str | None = None

    def head_object(self, **kwargs: Any) -> dict:
        """Read source size with the request retry budget."""
        return self._request("head_object", **kwargs)

    def copy_object(self, **kwargs: Any) -> dict:
        """Copy an object below the multipart threshold."""
        return self._request("copy_object", **kwargs)

    def create_multipart_upload(self, **kwargs: Any) -> dict:
        """Initiate the transfer; s3transfer keeps its UploadId and cleanup."""
        response = self._request("create_multipart_upload", **kwargs)
        self.upload_id = response["UploadId"]
        return response

    def upload_part_copy(self, **kwargs: Any) -> dict:
        """Retry the same UploadId, part number and source range."""
        return self._multipart_request("upload_part_copy", kwargs)

    def complete_multipart_upload(self, **kwargs: Any) -> dict:
        """Publish only the parts confirmed by s3transfer."""
        return self._multipart_request("complete_multipart_upload", kwargs)

    def _multipart_request(self, operation: str, parameters: dict) -> dict:
        try:
            return self._request(operation, **parameters)
        except Exception as error:
            # This error must survive probe fallback and process-pool transport.
            part = parameters.get("PartNumber")
            context = f", part {part}" if part is not None else ""
            raise S3MultipartCopyError(
                f"{operation} failed for s3://{parameters['Bucket']}/{parameters['Key']} "
                f"(UploadId {parameters['UploadId']}{context}): {error}"
            ) from error

    def abort_multipart_upload(self, **kwargs: Any) -> dict:
        """Make SDK cleanup observable without replacing the transfer error."""
        try:
            response = self._request("abort_multipart_upload", **kwargs)
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
