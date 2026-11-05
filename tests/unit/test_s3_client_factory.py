"""
S3 client lifecycle tests.
"""

import copy
import io
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.storage.async_pipeline.pipelines import copy_object_pipeline
from ch_backup.storage.engine import get_storage_engine
from ch_backup.storage.engine.s3 import s3_client_factory
from ch_backup.storage.engine.s3.s3_client_factory import S3ClientFactory
from ch_backup.storage.object_copy import CopyEntry


@pytest.mark.parametrize("server_side", [False, True])
def test_copy_pipelines_reuse_s3_clients(
    monkeypatch: pytest.MonkeyPatch, server_side: bool
) -> None:
    monkeypatch.setattr(s3_client_factory, "_FACTORIES", [])
    source = copy.deepcopy(DEFAULT_CONFIG["storage"])
    target = copy.deepcopy(source)
    source["credentials"]["bucket"] = "source"
    target["credentials"]["bucket"] = "target"
    data = b"data"
    reader, writer = Mock(), Mock()
    reader.head_object.return_value = {"ContentLength": len(data), "ETag": "etag"}
    reader.get_object.side_effect = lambda **_: {"Body": io.BytesIO(data)}
    with patch.object(s3_client_factory.boto3.session, "Session") as session:
        session.return_value.client.side_effect = [reader, writer]
        for index in range(3):
            entry = CopyEntry(
                copy.deepcopy(source),
                copy.deepcopy(target),
                f"src-{index}",
                f"dst-{index}",
                len(data),
                len(data),
                server_side,
            )
            copy_object_pipeline(DEFAULT_CONFIG, entry)
        copy_object_pipeline(
            DEFAULT_CONFIG,
            entry._replace(size=0, dst_size=0, dst_path="empty", server_side=False),
        )
        assert session.call_count == (1 if server_side else 2)
        assert session.return_value.client.call_count == session.call_count

    destination = reader if server_side else writer
    assert destination.copy.call_count == (3 if server_side else 0)
    assert [call.kwargs["Body"] for call in destination.put_object.call_args_list] == (
        [b""] if server_side else [data, data, data, b""]
    )


@pytest.mark.parametrize(
    "section, option, value",
    [
        ("credentials", "secret_access_key", "other-secret"),
        ("boto_config", "addressing_style", "path"),
    ],
)
def test_shared_s3_client_reset_and_config_isolation(
    monkeypatch: pytest.MonkeyPatch, section: str, option: str, value: Any
) -> None:
    monkeypatch.setattr(s3_client_factory, "_FACTORIES", [])
    config = copy.deepcopy(DEFAULT_CONFIG["storage"])
    config["retry_max_interval"] = 0
    with patch.object(s3_client_factory.boto3.session, "Session") as session:
        old_client, new_client, other_client = Mock(), Mock(), Mock()
        session.return_value.client.side_effect = [old_client, new_client, other_client]
        first = get_storage_engine(config)
        second = get_storage_engine(copy.deepcopy(config))
        first.upload_data(b"one", "one")
        second.upload_data(b"two", "two")
        assert session.call_count == session.return_value.client.call_count == 1
        assert getattr(first, "_multipart_downloads") is not getattr(
            second, "_multipart_downloads"
        )

        old_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "InternalError"}}, "PutObject"
        )
        first.upload_data(b"retry", "retry")
        second.upload_data(b"reused", "reused")
        assert new_client.put_object.call_count == 2
        assert session.call_count == 1
        assert session.return_value.client.call_count == 2

        config.setdefault(section, {})[option] = value
        get_storage_engine(config).upload_data(b"other", "other")
        assert other_client.put_object.call_count == 1
        assert session.call_count == 2
        assert (
            session.call_args.kwargs["aws_secret_access_key"]
            == config["credentials"]["secret_access_key"]
        )
        assert (
            session.return_value.client.call_args.kwargs["config"].s3[
                "addressing_style"
            ]
            == config["boto_config"]["addressing_style"]
        )


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
