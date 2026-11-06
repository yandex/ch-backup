"""
Connection lifecycle tests for S3 proxy discovery.
"""

import socket
import threading
import traceback
from base64 import b64encode
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import pytest
import requests

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.storage.engine.s3.s3_client_factory import S3ClientFactory


def test_proxy_discovery_opens_a_new_connection() -> None:
    connections: list[socket.socket] = []
    authorization: list[str | None] = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # pylint: disable=invalid-name
            authorization.append(self.headers.get("Authorization"))
            if self.path.endswith("/401"):
                self.send_error(401)
                return
            if self.connection not in connections:
                connections.append(self.connection)
            # Different valid proxy names, selected for each new connection.
            body = (
                b"127.0.0.1"
                if connections.index(self.connection) == 0
                else b"localhost"
            )
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    factory = S3ClientFactory(DEFAULT_CONFIG["storage"])
    resolve_proxies = getattr(factory, "_resolve_proxies")
    resolver = f"http://private-user:private-p%40ss@127.0.0.1:{server.server_port}/hostname_overlay"
    try:
        # Keep this local test independent of HTTP proxy environment settings.
        with (
            patch.dict(
                "os.environ",
                {
                    "NO_PROXY": "127.0.0.1",
                    "no_proxy": "127.0.0.1",
                    "NETRC": "/dev/null",
                },
            ),
            patch("ch_backup.storage.engine.s3.s3_client_factory.logging.debug") as log,
        ):
            first = resolve_proxies(resolver, 4080)
            second = resolve_proxies(resolver, 4080)
            with pytest.raises(requests.HTTPError) as exc:
                resolve_proxies(resolver + "/401", 4080)
        assert first == {"http": "127.0.0.1:4080", "https": "127.0.0.1:4080"}
        assert second == {"http": "localhost:4080", "https": "localhost:4080"}
        assert len(connections) == 2
        assert connections[0] is not connections[1]
        auth = "Basic " + b64encode(b"private-user:private-p@ss").decode()
        assert authorization == [auth] * 3
        output = str(log.mock_calls) + "".join(traceback.format_exception(exc.value))
        for secret in ("private-user", "private-p@ss", "private-p%40ss", auth):
            assert secret not in output
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
