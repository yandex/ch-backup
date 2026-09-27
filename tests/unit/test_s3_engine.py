"""Unit tests for S3 deletion and download failures."""

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from ch_backup.exceptions import StorageError
from ch_backup.storage.engine.s3.s3_engine import S3StorageEngine


def _engine() -> tuple[S3StorageEngine, MagicMock, MagicMock]:
    engine = S3StorageEngine.__new__(S3StorageEngine)
    client = MagicMock()
    factory = MagicMock()
    factory.create_s3_client.return_value = client
    engine._s3_client_factory = factory  # pylint: disable=protected-access
    engine._s3_bucket_name = "bucket"  # pylint: disable=protected-access
    engine._bulk_delete_enabled = True  # pylint: disable=protected-access
    return engine, client, factory


def test_bulk_delete_propagates_per_object_errors() -> None:
    engine, client, _ = _engine()
    client.delete_objects.return_value = {
        "Errors": [
            {"Key": "missing", "Code": "NoSuchKey"},
            {"Key": "denied", "Code": "AccessDenied", "Message": "denied"},
        ]
    }

    with pytest.raises(StorageError, match="denied"):
        engine.delete_files(["missing", "denied"])

    client.delete_objects.assert_called_once()


def test_bulk_delete_ignores_missing_objects() -> None:
    engine, client, _ = _engine()
    client.delete_objects.return_value = {
        "Errors": [{"Key": "missing", "Code": "NoSuchKey"}]
    }

    engine.delete_files(["missing"])


def test_missing_download_is_attempted_once() -> None:
    engine, client, factory = _engine()
    client.download_fileobj.side_effect = ClientError(
        {
            "Error": {"Code": "404", "Message": "Not Found"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        },
        "HeadObject",
    )

    with pytest.raises(ClientError):
        engine.download_data("missing")

    client.download_fileobj.assert_called_once()
    factory.reset.assert_not_called()
