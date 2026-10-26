"""Transport failures must not replay non-idempotent ClickHouse queries."""

import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace

import pytest

from ch_backup.clickhouse.client import ClickhouseClient, ClickhouseErrorNotRetriable


@pytest.fixture(name="transport")
def transport_fixture(monkeypatch):
    received = []
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # pylint: disable=invalid-name
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            if len(received) == 1:
                # Simulate a server applying DDL, then losing its response.
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok": 1}')

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    client = ClickhouseClient(
        {
            "host": "127.0.0.1",
            "protocol": "http",
            "port": server.server_port,
            "timeout": 2,
            "connect_timeout": 2,
        }
    )
    monkeypatch.setattr(ClickhouseClient.query.retry, "sleep", lambda _delay: None)
    try:
        yield SimpleNamespace(client=client, received=received)
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


@pytest.mark.parametrize(
    "statement",
    ["CREATE HANDLER test URL '/test' AS SELECT 1", "DROP HANDLER test"],
)
def test_connection_loss_does_not_replay_ddl(transport, statement):
    with pytest.raises(ClickhouseErrorNotRetriable):
        transport.client.query(statement, should_retry=False, sensitive=True)
    assert transport.received == [statement.encode()]


def test_connection_loss_does_not_replay_positional_non_retry_query(transport):
    with pytest.raises(ClickhouseErrorNotRetriable):
        transport.client.query("DROP HANDLER test", None, None, None, False)
    assert transport.received == [b"DROP HANDLER test"]


def test_connection_loss_still_retries_by_default(transport):
    assert transport.client.query("SELECT 1") == {"ok": 1}
    assert transport.received == [b"SELECT 1", b"SELECT 1"]
