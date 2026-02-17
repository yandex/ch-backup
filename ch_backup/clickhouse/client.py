"""
ClickHouse client.
"""

from contextlib import contextmanager
from typing import Any, Iterator, Optional, Union

import requests

from ch_backup import logging
from ch_backup.util import retry


class ClickhouseError(Exception):
    """
    ClickHouse interaction error.
    """


class ClickhouseErrorNotRetriable(ClickhouseError):
    """
    ClickHouse interaction error.
    """


class ClickhouseErrorRetriable(ClickhouseError):
    """
    ClickHouse interaction error.
    """


class ClickhouseClient:
    """
    ClickHouse client.
    """

    def __init__(self, config: dict, settings: dict = None) -> None:
        self._config = config
        self._settings = settings
        host = config["host"]
        protocol = config["protocol"]
        port = config["port"] or (8123 if protocol == "http" else 8443)
        self._session = self._create_session(config, settings)
        self._url = f"{protocol}://{host}:{port}"
        self.timeout = config["timeout"]
        self.connect_timeout = config["connect_timeout"]

    @property
    def settings(self):
        """
        ClickHouse settings.
        """
        return self._session.params

    @retry((requests.exceptions.ConnectionError, ClickhouseErrorRetriable))
    # pylint: disable=too-many-positional-arguments
    def query(
        self,
        query: Union[bytes, str],
        post_data: dict = None,
        settings: dict = None,
        timeout: float = None,
        should_retry: bool = True,
        new_session: bool = False,
        encoding: str = "utf-8",
    ) -> Any:
        """
        Execute query.
        """
        try:
            if timeout is None:
                timeout = self.timeout

            if isinstance(query, str):
                query = query.encode(encoding, "surrogateescape")

            logging.debug("Executing query: {}", query)

            # https://github.com/psf/requests/issues/2766
            # requests.Session object is not guaranteed to be thread-safe.
            # When using ClickhouseClient with multithreading, "new_session"
            # should be True, so a separate Session is used for each query.
            with self._get_session(new_session) as session:
                response = session.post(
                    self._url,
                    params=settings,
                    json=post_data,
                    timeout=(self.connect_timeout, timeout),
                    data=query,
                )

            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if should_retry:
                raise ClickhouseErrorRetriable(e.response.text.strip()) from e
            raise ClickhouseErrorNotRetriable(e.response.text.strip()) from e

        try:
            return response.json()
        except ValueError:
            return str.strip(response.text)

    @contextmanager
    def _get_session(
        self, new_session: Optional[bool] = False
    ) -> Iterator[requests.Session]:
        session = (
            self._create_session(self._config, self._settings)
            if new_session
            else self._session
        )
        try:
            yield session
        finally:
            if new_session:
                session.close()

    @staticmethod
    def _create_session(config, settings):
        session = requests.Session()

        ca_path = config.get("ca_path")
        session.verify = True if ca_path is None else ca_path

        headers = {}
        user = config.get("clickhouse_user")
        if user:
            headers["X-ClickHouse-User"] = user
        password = config.get("clickhouse_password")
        if password:
            headers["X-ClickHouse-Key"] = password

        session.headers.update(headers)

        if settings:
            session.params = settings

        requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

        return session
