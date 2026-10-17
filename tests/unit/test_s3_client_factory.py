"""
Connection lifecycle tests for S3 proxy discovery.
"""

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.storage.engine.s3.s3_client_factory import S3ClientFactory


def test_proxy_discovery_opens_a_new_connection() -> None:
    connections: list[socket.socket] = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # pylint: disable=invalid-name
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
    resolver = f"http://127.0.0.1:{server.server_port}/hostname_overlay"
    try:
        # Keep this local test independent of HTTP proxy environment settings.
        with patch.dict(
            "os.environ", {"NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"}
        ):
            first = resolve_proxies(resolver, 4080)
            second = resolve_proxies(resolver, 4080)
        assert first == {"http": "127.0.0.1:4080", "https": "127.0.0.1:4080"}
        assert second == {"http": "localhost:4080", "https": "localhost:4080"}
        assert len(connections) == 2
        assert connections[0] is not connections[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
