"""
ZooKeeper control unit tests.
"""

from unittest import mock

import pytest
from kazoo.exceptions import ConnectionLoss, NoNodeError
from tenacity import wait_none

from ch_backup.exceptions import TerminatingSignal
from ch_backup.zookeeper.zookeeper import ZookeeperClient, ZookeeperCTL


def _zk_ctl(children: dict[str, list[str]]) -> ZookeeperCTL:
    """Helper: a control object with the root /ch over the given child nodes."""

    def get_children(path: str) -> list[str]:
        """Answer the request from the given data like ZooKeeper does."""
        if path not in children:
            raise NoNodeError
        return children[path]

    with mock.patch("ch_backup.zookeeper.zookeeper.KazooClient") as client_cls:
        client_cls.return_value.get_children.side_effect = get_children
        return ZookeeperCTL({"hosts": ["zk"], "root_path": "/ch"})


def test_get_children_prepends_root_path() -> None:
    """
    A path is relative to the root, as the ZooKeeper paths of ClickHouse are.
    """
    zk_ctl = _zk_ctl({"/ch/zk/t1/replicas": ["r1", "r2"]})

    assert zk_ctl.get_children("/zk/t1/replicas") == ["r1", "r2"]


def test_get_children_of_missing_path_is_empty() -> None:
    """
    A table without replicas in ZooKeeper has no replicas path.
    """
    zk_ctl = _zk_ctl({})

    assert not zk_ctl.get_children("/zk/t1/replicas")


@pytest.fixture(name="client_context")
def _client_context():
    with (
        mock.patch(
            "ch_backup.zookeeper.zookeeper.KazooClient", autospec=True
        ) as factory,
        mock.patch.object(ZookeeperClient.__enter__.retry, "wait", wait_none()),
    ):
        yield ZookeeperClient(
            {"hosts": ["zk"], "user": "u", "password": "p", "connect_timeout": 10}
        ), factory.return_value


def test_nested_and_repeated_contexts(client_context):
    wrapper, client = client_context
    for _ in range(2):
        client.reset_mock()
        with wrapper as outer:
            with wrapper as inner:
                assert inner is outer is client
            client.stop.assert_not_called()
            client.close.assert_not_called()
        assert client.mock_calls == [
            mock.call.start(10),
            mock.call.add_auth("digest", "u:p"),
            mock.call.stop(),
            mock.call.close(),
        ]


@pytest.mark.parametrize("operation", ["start", "add_auth"])
def test_failed_connection_is_closed_before_retry(client_context, operation):
    wrapper, client = client_context
    getattr(client, operation).side_effect = [ConnectionLoss(), None]
    with wrapper as connected:
        assert connected is client
    first_attempt = [mock.call.start(10)]
    if operation == "add_auth":
        first_attempt.append(mock.call.add_auth("digest", "u:p"))
    assert client.mock_calls == first_attempt + [
        mock.call.stop(),
        mock.call.close(),
        mock.call.start(10),
        mock.call.add_auth("digest", "u:p"),
        mock.call.stop(),
        mock.call.close(),
    ]


@pytest.mark.parametrize(
    "stop_error,close_error,original_error",
    [
        (ConnectionLoss("stop"), None, None),
        (ConnectionLoss("stop"), OSError("close"), None),
        (None, OSError("close"), None),
        (ConnectionLoss("stop"), OSError("close"), TerminatingSignal()),
    ],
)
def test_cleanup_failure_preserves_error_and_prevents_reuse(
    client_context, stop_error, close_error, original_error
):
    wrapper, client = client_context
    client.stop.side_effect = stop_error
    client.close.side_effect = close_error
    expected = type(original_error) if original_error is not None else RuntimeError
    with pytest.raises(expected) as raised:
        with wrapper:
            if original_error is not None:
                raise original_error
    assert raised.value.__cause__ is (stop_error or close_error)
    if original_error is not None:
        assert raised.value is original_error
    assert client.mock_calls[-2:] == [mock.call.stop(), mock.call.close()]
    with pytest.raises(RuntimeError, match="cannot be reused"):
        with wrapper:
            pytest.fail("A client with failed cleanup must not be restarted")
    client.start.assert_called_once_with(10)
