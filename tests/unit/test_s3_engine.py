"""Unit tests for S3 deletion failures."""

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from ch_backup.exceptions import StorageError
from ch_backup.storage.engine.s3.s3_engine import S3StorageEngine


def _engine() -> tuple[S3StorageEngine, MagicMock]:
    engine = object.__new__(S3StorageEngine)
    client = MagicMock()
    factory = MagicMock()
    factory.create_s3_client.return_value = client
    engine._s3_client_factory = factory  # pylint: disable=protected-access
    engine._s3_bucket_name = "bucket"  # pylint: disable=protected-access
    engine._bulk_delete_enabled = True  # pylint: disable=protected-access
    return engine, client


def test_bulk_delete_propagates_per_object_errors() -> None:
    engine, client = _engine()
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
    engine, client = _engine()
    client.delete_objects.return_value = {
        "Errors": [{"Key": "missing", "Code": "NoSuchKey"}]
    }

    engine.delete_files(["missing"])


@pytest.mark.parametrize("operation", ["download_file", "download_data"])
def test_missing_download_is_not_retried(operation: str) -> None:
    engine, client = _engine()
    error = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject")
    download = getattr(
        client, "download_fileobj" if operation == "download_data" else operation
    )
    download.side_effect = error

    with pytest.raises(ClientError) as exc:
        if operation == "download_file":
            engine.download_file("missing", "/tmp/missing")
        else:
            engine.download_data("missing")

    assert exc.value is error
    download.assert_called_once()
