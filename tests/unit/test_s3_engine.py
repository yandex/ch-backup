"""
Unit tests for S3StorageEngine retries.
"""

import socket
import ssl
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests
from botocore.exceptions import ClientError
from botocore.exceptions import SSLError as BotoSSLError
from urllib3 import HTTPConnectionPool
from urllib3.exceptions import MaxRetryError
from urllib3.exceptions import SSLError as Urllib3SSLError
from urllib3.util.ssl_match_hostname import CertificateError as Urllib3CertificateError

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.s3 import S3RetryingError, S3StorageEngine
from ch_backup.storage.engine.s3.s3_client_factory import (
    S3BalancerUnknownHost,
    S3ClientCachedFactory,
)


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
    assert isinstance(exc.value.__context__, S3BalancerUnknownHost)
    assert exc.value.__context__.__context__ is dns_error


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
            for code in (400, 401, 403, 404, 507)
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


@pytest.mark.parametrize("error_code", ["507", "InternalError", "InsufficientStorage"])
@pytest.mark.parametrize(
    ("operation", "client_method"),
    [
        ("download_data", "download_fileobj"),
        ("download_part", "get_object"),
        ("path_exists", "head_object"),
    ],
)
def test_s3_http_507_is_not_retried(
    error_code: str, operation: str, client_method: str
) -> None:
    engine = _engine(retry_max_attempts=3)
    sleep = Mock()
    getattr(engine, "_retry").sleep_function = sleep
    client = Mock()
    factory = Mock(create_s3_client=Mock(return_value=client))
    setattr(engine, "_s3_client_factory", factory)
    argument = "path"
    if operation == "download_part":
        client.head_object.return_value = {"ContentLength": 1, "ETag": "etag"}
        argument = engine.create_multipart_download("path")

    error = ClientError(
        {
            "Error": {"Code": error_code, "Message": "Object data unavailable"},
            "ResponseMetadata": {"HTTPStatusCode": 507},
        },
        client_method,
    )
    request = getattr(client, client_method)
    request.side_effect = error

    with pytest.raises(ClientError) as exc:
        getattr(engine, operation)(argument)

    assert exc.value is error
    request.assert_called_once()
    factory.reset.assert_not_called()
    sleep.assert_not_called()


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (500, "InternalError"),
        (503, "ServiceUnavailable"),
        (400, "RequestTimeout"),
        (400, "Throttling"),
        (503, "SlowDown"),
        (408, "408"),
        (429, "429"),
    ],
)
def test_s3_server_error_is_retried(status: int, code: str) -> None:
    engine = _engine(retry_max_attempts=2)
    sleep = Mock()
    getattr(engine, "_retry").sleep_function = sleep
    client = Mock()
    client.download_fileobj.side_effect = [
        ClientError(
            {
                "Error": {"Code": code},
                "ResponseMetadata": {"HTTPStatusCode": status},
            },
            "GetObject",
        ),
        None,
    ]
    factory = Mock(create_s3_client=Mock(return_value=client))
    setattr(engine, "_s3_client_factory", factory)

    assert engine.download_data("path") == b""
    assert client.download_fileobj.call_count == 2
    factory.reset.assert_called_once_with()
    sleep.assert_called_once()


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (403, "AccessDenied"),
        (403, "InvalidAccessKeyId"),
        (403, "SignatureDoesNotMatch"),
        (400, "InvalidArgument"),
        (400, "MalformedXML"),
        (404, "NoSuchKey"),
        (404, "NotFound"),
        (403, "403"),
        (400, "UnknownBadRequest"),
        (None, "AccessDenied"),
        (None, "InvalidAccessKeyId"),
        (None, "SignatureDoesNotMatch"),
        (None, "507"),
        (None, "InsufficientStorage"),
    ],
)
def test_s3_permanent_response_is_not_retried(status: int | None, code: str) -> None:
    engine = _engine(retry_max_attempts=3)
    sleep = Mock()
    getattr(engine, "_retry").sleep_function = sleep
    error = ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "GetObject",
    )
    client = Mock()
    client.download_fileobj.side_effect = error
    factory = Mock(create_s3_client=Mock(return_value=client))
    setattr(engine, "_s3_client_factory", factory)

    with pytest.raises(ClientError) as exc:
        engine.download_data("path")

    assert exc.value is error
    client.download_fileobj.assert_called_once()
    factory.reset.assert_not_called()
    sleep.assert_not_called()


@pytest.mark.parametrize("boto", [False, True])
@pytest.mark.parametrize(
    ("cause", "retryable"),
    [
        (ssl.SSLCertVerificationError(1, "certificate verify failed"), False),
        (Urllib3CertificateError("hostname does not match"), False),
        (ssl.SSLEOFError(8, "connection closed during TLS handshake"), True),
    ],
)
def test_tls_verification_failure_is_not_retried(
    boto: bool, cause: Exception, retryable: bool
) -> None:
    engine = _engine(retry_max_attempts=2)
    sleep = Mock()
    getattr(engine, "_retry").sleep_function = sleep
    wrapped = Urllib3SSLError(cause)
    error = (
        BotoSSLError(endpoint_url="https://s3", error=wrapped)
        if boto
        else requests.exceptions.SSLError(
            MaxRetryError(HTTPConnectionPool("resolver"), "/resolver", wrapped)
        )
    )
    client = Mock()
    factory = Mock(create_s3_client=Mock(side_effect=[error, client]))
    setattr(engine, "_s3_client_factory", factory)

    if retryable:
        assert engine.download_data("path") == b""
        assert factory.create_s3_client.call_count == 2
        sleep.assert_called_once()
    else:
        with pytest.raises(type(error)) as exc:
            engine.download_data("path")
        assert exc.value is error
        factory.create_s3_client.assert_called_once_with()
        factory.reset.assert_not_called()
        sleep.assert_not_called()


@pytest.mark.parametrize("discovery_failure", [True, False])
def test_delayed_reset_preserves_another_threads_client(
    discovery_failure: bool,
) -> None:
    engine = _engine(retry_max_attempts=2)
    getattr(engine, "_retry").sleep_function = Mock()
    old_client, healthy_client = Mock(), Mock()
    old_client.download_fileobj.side_effect = ClientError(
        {"Error": {"Code": "InternalError"}}, "GetObject"
    )
    first_result = (
        S3BalancerUnknownHost("proxy DNS failed") if discovery_failure else old_client
    )
    source = Mock(create_s3_client=Mock(side_effect=[first_result, healthy_client]))
    cache = S3ClientCachedFactory(source)
    setattr(engine, "_s3_client_factory", cache)
    original_reset = cache.reset
    reset_started, allow_reset = threading.Event(), threading.Event()

    def delayed_reset() -> None:
        reset_started.set()
        assert allow_reset.wait(5)
        original_reset()

    with (
        patch.object(cache, "reset", side_effect=delayed_reset),
        ThreadPoolExecutor(1) as pool,
    ):
        failing_operation = pool.submit(engine.download_data, "first")
        try:
            assert reset_started.wait(5)
            # Model a second worker replacing the old client before A handles its error.
            assert cache.create_s3_client(cached=False) is healthy_client
            assert engine.download_data("second") == b""
        finally:
            allow_reset.set()
        assert failing_operation.result(timeout=5) == b""

    assert source.create_s3_client.call_count == 2
    assert healthy_client.download_fileobj.call_count == 2


def test_failed_s3_request_replaces_its_own_cached_client() -> None:
    engine = _engine(retry_max_attempts=2)
    getattr(engine, "_retry").sleep_function = Mock()
    broken, healthy = Mock(), Mock()
    broken.download_fileobj.side_effect = ClientError(
        {"Error": {"Code": "InternalError"}}, "GetObject"
    )
    source = Mock(create_s3_client=Mock(side_effect=[broken, healthy]))
    setattr(engine, "_s3_client_factory", S3ClientCachedFactory(source))

    assert engine.download_data("path") == b""
    assert source.create_s3_client.call_count == 2
    broken.download_fileobj.assert_called_once()
    healthy.download_fileobj.assert_called_once()
