"""
S3 client lifecycle tests.
"""

import copy
import io
import socket
import threading
import traceback
from base64 import b64encode
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import Mock, patch

import pytest
import requests
from botocore.exceptions import ClientError

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError
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
    assert destination.copy_object.call_count == (3 if server_side else 0)
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
    config["client_ttl"] = 1
    config["retry_max_interval"] = 0
    now = Mock(return_value=0.0)
    monkeypatch.setattr(s3_client_factory, "monotonic", now)
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
        with pytest.raises(ConfigurationError, match=r"storage\.client_ttl"):
            get_storage_engine({**config, "client_ttl": True})

        refreshed = Mock()
        session.return_value.client.side_effect = [
            s3_client_factory.S3BalancerUnknownHost("proxy discovery failed"),
            refreshed,
        ]
        now.return_value = config["client_ttl"] / 2
        second.upload_data(b"before expiry", "before-expiry")
        assert session.return_value.client.call_count == 3
        now.return_value = config["client_ttl"]
        second.upload_data(b"failed refresh", "failed-refresh")
        second.upload_data(b"reuse", "reuse")
        assert session.return_value.client.call_count == 4
        assert new_client.put_object.call_count == 5
        now.return_value += config["client_ttl"]
        second.upload_data(b"refreshed", "refreshed")
        assert session.return_value.client.call_count == 5
        assert refreshed.put_object.call_count == 1
        assert session.call_count == 2


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
