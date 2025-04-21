"""
ClickHouse client.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import requests

from ch_backup import logging
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ExecPool
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
        query: str,
        post_data: dict = None,
        settings: dict = None,
        timeout: float = None,
        should_retry: bool = True,
        new_session: bool = False,
    ) -> Any:
        """
        Execute query.
        """
        try:
            logging.debug("Executing query: {}", query)

            if timeout is None:
                timeout = self.timeout

            if new_session:
                with self._create_session(self._config, self.settings) as session:
                    response = session.post(
                        self._url,
                        params=settings,
                        json=post_data,
                        timeout=(self.connect_timeout, timeout),
                        data=query.encode("utf-8"),
                    )
            else:
                response = self._session.post(
                    self._url,
                    params=settings,
                    json=post_data,
                    timeout=(self.connect_timeout, timeout),
                    data=query.encode("utf-8"),
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


class ClickhouseClientMultithreading:
    """
    ClickHouse client with multithreading support.
    """

    def __init__(self, ch_client: ClickhouseClient, threads: int) -> None:
        self._ch_client = ch_client
        self._pool = ExecPool(ThreadPoolExecutor(max_workers=threads))

    # pylint: disable=too-many-positional-arguments
    def submit_query(
        self,
        query: str,
        post_data: dict = None,
        settings: dict = None,
        timeout: float = None,
        should_retry: bool = True,
    ) -> Any:
        """
        Submit query to the pool.
        """
        self._pool.submit(
            job_id=query,
            func=self._ch_client.query,
            callback=None,
            query=query,
            post_data=post_data,
            settings=settings,
            timeout=timeout,
            should_retry=should_retry,
            new_session=True,
        )

    def wait_all(
        self,
        keep_going: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Wait until all submitted queries are finished.
        """
        self._pool.wait_all(keep_going, timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._pool.shutdown(graceful=True)
        except Exception:  # nosec B110
            pass
        return False
