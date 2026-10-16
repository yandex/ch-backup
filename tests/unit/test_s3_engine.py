"""
Unit tests for S3StorageEngine retries.
"""

import socket
from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests
from botocore.exceptions import ClientError

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.s3 import S3RetryingError, S3StorageEngine
from ch_backup.storage.engine.s3.s3_client_factory import S3BalancerUnknownHost


def _engine(**storage: Any) -> S3StorageEngine:
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


def _response(status: int = 200, text: str = "proxy.example\n") -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = text.encode()  # pylint: disable=protected-access
    setattr(response, "_content_consumed", True)
    return response


@pytest.mark.parametrize("dns_errno", [socket.EAI_AGAIN, socket.EAI_NODATA])
def test_proxy_dns_failure_recovers_after_six_attempts(dns_errno: int) -> None:
    engine = _engine(proxy_resolver={"uri": "http://resolver", "proxy_port": 4080})
    getattr(engine, "_retry").sleep_function = Mock()
    factory = getattr(getattr(engine, "_s3_client_factory"), "_s3_client_factory")
    client = Mock()
    client.download_fileobj.side_effect = lambda _bucket, _key, target: target.write(
        b"backup data"
    )
    dns_error = socket.gaierror(dns_errno, "DNS lookup failed")
    with (
        patch(
            "ch_backup.storage.engine.s3.s3_client_factory.requests.get",
            side_effect=[_response(text=f"proxy-{i}.example\n") for i in range(7)],
        ) as get,
        patch(
            "ch_backup.storage.engine.s3.s3_client_factory.socket.getaddrinfo",
            side_effect=[dns_error] * 6 + [[]],
        ) as resolve,
        patch.object(getattr(factory, "_s3_session"), "client", return_value=client),
    ):
        assert engine.download_data("path") == b"backup data"
        # Once created, the working S3 client is reused for the next operation.
        assert engine.download_data("another-path") == b"backup data"

    assert get.call_count == 7
    assert [call.args[0] for call in resolve.call_args_list] == [
        f"proxy-{i}.example" for i in range(7)
    ]
    assert all(call.kwargs["timeout"] == 10 for call in get.call_args_list)
    assert getattr(engine, "_retry").sleep_function.call_count == 6


@pytest.mark.parametrize("max_attempts", [1, 3])
def test_proxy_dns_failure_uses_one_retry_budget(max_attempts: int) -> None:
    engine = _engine(
        retry_max_attempts=max_attempts,
        proxy_resolver={"uri": "http://resolver", "proxy_port": 4080, "timeout": 2},
    )
    getattr(engine, "_retry").sleep_function = Mock()
    dns_error = socket.gaierror(
        socket.EAI_NODATA, "No address associated with hostname"
    )
    with (
        patch(
            "ch_backup.storage.engine.s3.s3_client_factory.requests.get",
            side_effect=lambda *args, **kwargs: _response(),
        ) as get,
        patch(
            "ch_backup.storage.engine.s3.s3_client_factory.socket.getaddrinfo",
            side_effect=dns_error,
        ),
        pytest.raises(S3RetryingError, match="DNS error:.*No address") as exc,
    ):
        engine.download_data("path")

    assert get.call_count == max_attempts
    assert all(call.kwargs["timeout"] == 2 for call in get.call_args_list)
    assert getattr(engine, "_retry").sleep_function.call_count == max_attempts - 1
    assert isinstance(exc.value.__cause__, S3BalancerUnknownHost)
    assert exc.value.__cause__.__cause__ is dns_error


@pytest.mark.parametrize(
    "error",
    [
        requests.ConnectTimeout("connect timed out"),
        requests.ReadTimeout("read timed out"),
        requests.ConnectionError("connection reset"),
        requests.exceptions.ChunkedEncodingError("response body interrupted"),
        *[
            requests.HTTPError(response=_response(code))
            for code in (408, 429, 500, 503)
        ],
    ],
)
def test_proxy_request_failure_is_retried(error: Exception) -> None:
    engine = _engine(retry_max_attempts=2)
    getattr(engine, "_retry").sleep_function = Mock()
    client = Mock()
    factory = Mock(create_s3_client=Mock(side_effect=[error, client]))
    setattr(engine, "_s3_client_factory", factory)

    assert engine.download_data("path") == b""
    assert factory.create_s3_client.call_count == 2
    factory.reset.assert_called_once_with()


@pytest.mark.parametrize(
    "error",
    [
        *[
            requests.HTTPError(response=_response(code))
            for code in (400, 401, 403, 404)
        ],
        requests.exceptions.InvalidURL("invalid resolver URL"),
        ValueError("invalid configuration"),
        ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject"),
    ],
)
def test_permanent_error_is_not_retried(error: Exception) -> None:
    engine = _engine(retry_max_attempts=3)
    getattr(engine, "_retry").sleep_function = Mock()
    factory = Mock(create_s3_client=Mock(side_effect=error))
    setattr(engine, "_s3_client_factory", factory)

    with pytest.raises(type(error)) as exc:
        engine.download_data("path")

    assert exc.value is error
    factory.create_s3_client.assert_called_once_with()
    factory.reset.assert_not_called()
    getattr(engine, "_retry").sleep_function.assert_not_called()
