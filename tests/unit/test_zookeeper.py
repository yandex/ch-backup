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
@pytest.mark.parametrize("stop_fails", [False, True])
def test_failed_connection_is_closed_before_retry(
    client_context, operation, stop_fails
):
    wrapper, client = client_context
    getattr(client, operation).side_effect = [ConnectionLoss(), None]
    if stop_fails:
        client.stop.side_effect = [BrokenPipeError("stop failed"), None]
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
    "failure_at,original_error",
    [
        pytest.param("start", OSError(24, "Too many open files"), id="start-emfile"),
        pytest.param("start", TerminatingSignal(), id="start-signal"),
        pytest.param("body", ValueError("operation failed"), id="body-error"),
        pytest.param("body", TerminatingSignal(), id="body-signal"),
        pytest.param("body", None, id="exit"),
    ],
)
@pytest.mark.parametrize("stop_fails", [False, True], ids=["close", "stop-and-close"])
def test_cleanup_failure_preserves_errors_and_allows_reuse(
    client_context, failure_at, original_error, stop_fails
):
    wrapper, client = client_context
    stop_error = (
        AttributeError("socket unavailable")
        if failure_at == "start"
        else BrokenPipeError("stop failed")
    )
    close_error = OSError("close failed")
    if stop_fails:
        client.stop.side_effect = stop_error
    client.close.side_effect = close_error
    if failure_at == "start":
        client.start.side_effect = original_error
    expected = (
        original_error
        if failure_at == "start" or isinstance(original_error, TerminatingSignal)
        else stop_error if stop_fails else close_error
    )
    with pytest.raises(type(expected)) as raised:
        with wrapper:
            assert failure_at != "start", "A failed start must not enter the context"
            if original_error is not None:
                raise original_error
    assert raised.value is expected
    if failure_at == "body" and isinstance(original_error, Exception):
        assert raised.value.__context__ is original_error
        assert not raised.value.__suppress_context__
    assert client.mock_calls[-2:] == [mock.call.stop(), mock.call.close()]

    client.reset_mock(side_effect=True)
    with wrapper as connected:
        assert connected is client
    client.start.assert_called_once_with(10)
    client.stop.assert_called_once_with()
    client.close.assert_called_once_with()
