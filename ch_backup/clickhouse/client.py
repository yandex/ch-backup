"""
ClickHouse client.
"""
from typing import Any

import requests

from ch_backup import logging
from ch_backup.util import retry


class ClickhouseError(Exception):
    """
    ClickHouse interaction error.
    """


class ClickhouseClient:
    """
    ClickHouse client.
    """
    def __init__(self, config: dict, settings: dict = None) -> None:
        host = config['host']
        protocol = config['protocol']
        port = config['port'] or (8123 if protocol == 'http' else 8443)
        self._session = self._create_session(config, settings)
        self._url = f'{protocol}://{host}:{port}'
        self.timeout = config['timeout']
        self.connect_timeout = config['connect_timeout']

    @property
    def settings(self):
        """
        ClickHouse settings.
        """
        return self._session.params

    @retry(requests.exceptions.ConnectionError)
    def query(self, query: str, post_data: dict = None, settings: dict = None, timeout: float = None) -> Any:
        """
        Execute query.
        """
        try:
            logging.debug('Executing query: %s', query)

            if timeout is None:
                timeout = self.timeout

            response = self._session.post(self._url,
                                          params=settings,
                                          json=post_data,
                                          timeout=(self.connect_timeout, timeout),
                                          data=query.encode('utf-8'))

            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise ClickhouseError(e.response.text.strip()) from e

        try:
            return response.json()
        except ValueError:
            return str.strip(response.text)

    @staticmethod
    def _create_session(config, settings):
        session = requests.Session()

        ca_path = config.get('ca_path')
        session.verify = True if ca_path is None else ca_path

        headers = {}
        user = config.get('clickhouse_user')
        if user:
            headers['X-ClickHouse-User'] = user
        password = config.get('clickhouse_password')
        if password:
            headers['X-ClickHouse-Key'] = password

        session.headers.update(headers)

        if settings:
            session.params = settings

        # pylint: disable=no-member
        requests.packages.urllib3.disable_warnings()

        return session
