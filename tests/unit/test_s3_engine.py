"""
Unit tests for S3StorageEngine retries.
"""

from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.s3 import S3RetryingError, S3StorageEngine


def _engine(**storage: float) -> S3StorageEngine:
    """Build an engine on the default storage config with the given overrides."""
    config: dict = {**DEFAULT_CONFIG["storage"], **storage}  # type: ignore[dict-item]
    config["credentials"] = {**config["credentials"], "bucket": "bucket"}
    return S3StorageEngine(config)


@pytest.mark.parametrize("max_attempts", [1, 3])
def test_retry_max_attempts_from_config(max_attempts: int) -> None:
    """S3 operations are retried with the attempt and pause limits from the config."""
    engine = _engine(retry_max_attempts=max_attempts, retry_max_interval=1.5)
    client = Mock()
    client.download_fileobj.side_effect = ClientError(
        {"Error": {"Code": "InternalError"}}, "GetObject"
    )
    setattr(
        engine, "_s3_client_factory", Mock(create_s3_client=Mock(return_value=client))
    )
    sleep = Mock()
    getattr(engine, "_retry").sleep_function = sleep

    with (
        patch(
            "ch_backup.storage.engine.s3.s3_retry.uniform",
            side_effect=lambda _low, high: high,
        ),
        pytest.raises(S3RetryingError),
    ):
        engine.download_data("path")

    assert client.download_fileobj.call_count == max_attempts
    assert sleep.call_count == max_attempts - 1
    assert all(call.args[0] <= 1.5 for call in sleep.call_args_list)


@pytest.mark.parametrize(
    "storage",
    [
        {"retry_max_attempts": 0},
        {"retry_max_attempts": 2.5},
        {"retry_max_interval": -1},
    ],
)
def test_invalid_retry_limits_are_rejected(storage: dict) -> None:
    """Retry limits that would end retries without an error or crash sleep are rejected."""
    with pytest.raises(ConfigurationError, match="storage.retry_"):
        _engine(**storage)
