"""Unit tests for S3 deletion failures."""

from unittest.mock import MagicMock

import pytest

from ch_backup.exceptions import StorageError
from ch_backup.storage.engine.s3.s3_engine import S3StorageEngine


def _engine() -> tuple[S3StorageEngine, MagicMock]:
    engine = S3StorageEngine.__new__(S3StorageEngine)
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
