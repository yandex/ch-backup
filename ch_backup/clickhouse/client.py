"""
ClickHouse client.
"""

import re
from contextlib import contextmanager
from typing import Any, Generator

import requests

from ch_backup import logging
from ch_backup.util import retry


class ClickhouseError(Exception):
    """
    ClickHouse interaction error.
    """

    def __init__(self, message: str, *, server_message: str | None = None) -> None:
        """Keep the server response for error handling, separate from printable text."""
        super().__init__(message)
        self.server_message = message if server_message is None else server_message


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
        query: bytes | str,
        post_data: dict = None,
        settings: dict = None,
        timeout: float = None,
        should_retry: bool = True,
        new_session: bool = False,
        encoding: str = "utf-8",
        sensitive: bool = False,
    ) -> Any:
        """
        Execute query.
        """
        try:
            if timeout is None:
                timeout = self.timeout

            if isinstance(query, str):
                query = query.encode(encoding, "surrogateescape")

            if sensitive:
                logging.debug("Executing sensitive query: <{} bytes>", len(query))
            else:
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
        except requests.exceptions.ConnectionError as e:
            if should_retry:
                raise
            raise ClickhouseErrorNotRetriable(
                "ClickHouse connection failed; query was not retried"
            ) from e
        except requests.exceptions.HTTPError as e:
            error_text = e.response.text.strip()
            code = re.match(r"Code: [0-9]{1,6}\.", error_text)
            message = f"{code[0]} " if code else ""
            message += "ClickHouse query failed; server response hidden"
            if should_retry:
                raise ClickhouseErrorRetriable(
                    message, server_message=error_text
                ) from None
            raise ClickhouseErrorNotRetriable(
                message, server_message=error_text
            ) from None

        try:
            return response.json()
        except ValueError:
            return str.strip(response.text)

    @contextmanager
    def _get_session(
        self, new_session: bool | None = False
    ) -> Generator[requests.Session, None, None]:
        session = (
            self._create_session(self._config, dict(self._session.params))
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
        """Create a session after validating credentials without exposing their values."""
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

        for name, value in headers.items():
            try:
                requests.utils.check_header_validity((name, value))
                encoded = value.encode("latin-1") if isinstance(value, str) else value
                if b"\r" in encoded or b"\n" in encoded:
                    raise ValueError
            except ValueError:
                raise ClickhouseErrorNotRetriable(
                    f"Invalid ClickHouse HTTP header {name}"
                ) from None

        session.headers.update(headers)

        if settings:
            session.params = settings

        requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

        return session
