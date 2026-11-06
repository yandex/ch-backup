"""
Connection lifecycle tests for S3 proxy discovery.
"""

import socket
import threading
import traceback
from base64 import b64encode
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import requests
from loguru import logger

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.s3 import S3RetryingError, S3StorageEngine
from ch_backup.storage.engine.s3.s3_client_factory import S3ClientFactory


@pytest.fixture(name="resolver")
def resolver_fixture():
    connections: list[socket.socket] = []
    received: list[tuple[str, str | None]] = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        close_connection = False

        def do_GET(self) -> None:  # pylint: disable=invalid-name
            received.append((self.path, self.headers.get("Authorization")))
            if self.path == "/timeout":
                # Let the client reach its read timeout without writing to the
                # connection it has already closed.
                self.close_connection = True
                threading.Event().wait(0.3)
                return
            if self.connection not in connections:
                connections.append(self.connection)
            # Different valid proxy names, selected for each new connection.
            body = (
                b"127.0.0.1"
                if connections.index(self.connection) == 0
                else b"localhost"
            )
            status = 200
            if self.path in ("/401", "/503"):
                status = int(self.path[1:])
            if self.path == "/bad-host":
                body = b"invalid/hostname"
            if self.path in ("/redirect", "/cross-host"):
                status = 302
            self.send_response(status)
            if self.path == "/redirect":
                self.send_header("Location", "/503")
            if self.path == "/cross-host":
                self.send_header(
                    "Location", f"http://localhost:{server.server_port}/503"
                )
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with patch.dict(
            "os.environ",
            {
                "NO_PROXY": "127.0.0.1,localhost",
                "no_proxy": "127.0.0.1,localhost",
                "NETRC": "/dev/null",
            },
        ):
            yield SimpleNamespace(
                url=f"http://127.0.0.1:{server.server_port}",
                received=received,
                connections=connections,
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_proxy_discovery_opens_a_new_connection(resolver: SimpleNamespace) -> None:
    factory = S3ClientFactory(DEFAULT_CONFIG["storage"])
    resolve_proxies = getattr(factory, "_resolve_proxies")
    uri = resolver.url + "/hostname_overlay"
    first = resolve_proxies(uri, 4080)
    second = resolve_proxies(uri, 4080)
    assert first == {"http": "127.0.0.1:4080", "https": "127.0.0.1:4080"}
    assert second == {"http": "localhost:4080", "https": "localhost:4080"}
    assert len(resolver.connections) == 2
    assert resolver.connections[0] is not resolver.connections[1]


@pytest.mark.parametrize(
    "path,attempts",
    [
        ("/401", 1),
        ("/503", 2),
        ("/redirect", 2),
        ("/cross-host", 2),
        ("/bad-host", 2),
        ("/timeout", 2),
    ],
)
def test_resolver_credentials_do_not_reach_errors_or_logs(
    resolver, path, attempts, caplog
):
    uri = resolver.url.replace("//", "//private-user:private-p%40ss@") + path
    storage = {
        **DEFAULT_CONFIG["storage"],
        "credentials": {
            "access_key_id": "dummy",
            "secret_access_key": "dummy",
            "bucket": "bucket",
        },
        "proxy_resolver": {
            "uri": uri,
            "proxy_port": 4080,
            "timeout": 0.05 if path == "/timeout" else 10,
        },
        "retry_max_attempts": 2,
        "retry_max_interval": 0,
    }
    engine = S3StorageEngine(storage)
    logs: list[str] = []
    sink = logger.add(logs.append, format="{message}", diagnose=False, backtrace=False)
    caplog.set_level("DEBUG", logger="urllib3")
    try:
        with pytest.raises(
            requests.HTTPError if path == "/401" else S3RetryingError
        ) as exc:
            engine.path_exists("object")
    finally:
        logger.remove(sink)
    auth = "Basic " + b64encode(b"private-user:private-p@ss").decode()
    assert [item for item in resolver.received if item[0] == path] == [
        (path, auth)
    ] * attempts
    if path in ("/redirect", "/cross-host"):
        assert [item[1] for item in resolver.received if item[0] == "/503"] == [
            auth if path == "/redirect" else None
        ] * attempts
    if path == "/401":
        assert isinstance(exc.value, requests.HTTPError)
        assert exc.value.response is not None
        assert exc.value.response.status_code == 401
    output = (
        "".join(logs) + caplog.text + "".join(traceback.format_exception(exc.value))
    )
    for secret in ("private-user", "private-p@ss", "private-p%40ss", auth):
        assert secret not in output
    assert "127.0.0.1" in output


@pytest.mark.parametrize(
    "userinfo,path",
    [
        ("", "/"),
        ("private-user:private-password@", "/"),
        ("private-user:private-password@", "/redirect"),
    ],
)
def test_resolver_preserves_requests_auth_with_netrc(
    resolver, tmp_path, userinfo, path
):
    netrc = tmp_path / "netrc"
    netrc.write_text("machine 127.0.0.1 login netrc-user password netrc-password\n")
    uri = "  " + resolver.url.replace("//", f"//{userinfo}") + path
    logs: list[str] = []
    sink = logger.add(logs.append, format="{message}", diagnose=False)
    try:
        with patch.dict("os.environ", {"NETRC": str(netrc)}):
            # Netrc precedence differs across Requests releases. Compare the
            # headers observed by the server with the unmodified client.
            with requests.get(uri, timeout=10):
                pass
            expected_requests = resolver.received[:]
            resolver.received.clear()
            resolve = getattr(
                S3ClientFactory(DEFAULT_CONFIG["storage"]), "_resolve_proxies"
            )
            if path == "/redirect":
                with pytest.raises(requests.HTTPError) as exc:
                    resolve(uri, 4080)
                assert "private-" not in "".join(traceback.format_exception(exc.value))
            else:
                assert resolve(uri, 4080) == {
                    "http": "localhost:4080",
                    "https": "localhost:4080",
                }
    finally:
        logger.remove(sink)
    assert resolver.received == expected_requests
    assert all(auth is not None for _, auth in resolver.received)
    assert "private-" not in "".join(logs)
    assert "netrc-password" not in "".join(logs)


@pytest.mark.parametrize(
    "uri",
    [
        "http://private-user:private-password@localhost:bad",
        "http://private-user:private-password@[broken",
        "ftp://private-user:private-password@localhost",
        "private-user:private-password@localhost",
    ],
)
def test_invalid_resolver_uri_has_safe_error(uri):
    factory = S3ClientFactory(DEFAULT_CONFIG["storage"])
    with pytest.raises(ConfigurationError, match="storage.proxy_resolver.uri") as exc:
        getattr(factory, "_resolve_proxies")(uri, 4080)
    assert "private-" not in "".join(traceback.format_exception(exc.value))
