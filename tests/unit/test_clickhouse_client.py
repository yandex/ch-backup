"""Transport retries must honor the caller's idempotency decision."""

# pylint: disable=protected-access

from unittest.mock import MagicMock

import pytest
from requests import Response
from requests.exceptions import ConnectionError as RequestsConnectionError
from tenacity import wait_none

from ch_backup.clickhouse.client import ClickhouseClient
from ch_backup.config import DEFAULT_CONFIG


@pytest.mark.parametrize(
    "statement", ["CREATE DATABASE scratch", "DROP DATABASE scratch SYNC"]
)
@pytest.mark.parametrize("should_retry", [False, True])
@pytest.mark.parametrize("positional", [False, True])
def test_connection_retry_respects_should_retry(statement, should_retry, positional):
    client = ClickhouseClient(DEFAULT_CONFIG["clickhouse"])
    response = Response()
    response.status_code = 200
    response._content = b"OK"
    client._session = MagicMock()
    client._session.post.side_effect = [
        RequestsConnectionError("response lost"),
        response,
    ]
    # Tenacity adds retry_with to the decorated method at runtime.
    # pylint: disable-next=no-member
    query = ClickhouseClient.query.retry_with(wait=wait_none())
    args = [client, statement]
    kwargs = {}
    if positional:
        args.extend([None, None, None, should_retry])
    else:
        kwargs["should_retry"] = should_retry
    if should_retry:
        assert query(*args, **kwargs) == "OK"
        assert client._session.post.call_count == 2
    else:
        with pytest.raises(RequestsConnectionError, match="response lost"):
            query(*args, **kwargs)
        assert client._session.post.call_count == 1
