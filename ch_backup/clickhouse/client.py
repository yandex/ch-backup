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

    def __init__(self, config: dict) -> None:
        host = config['host']
        protocol = config['protocol']
        port = 8123 if protocol == 'http' else 8443
        ca_path = config.get('ca_path')
        self._session = requests.Session()
        self._session.verify = True if ca_path is None else ca_path
        # pylint: disable=no-member
        requests.packages.urllib3.disable_warnings()
        self._url = '{0}://{1}:{2}'.format(protocol, host, port)
        self._timeout = config['timeout']

    @retry(requests.exceptions.ConnectionError)
    def query(self, query: str, post_data: dict = None) -> Any:
        """
        Execute query.
        """
        try:
            logging.debug('Executing query: %s', query)
            response = self._session.post(self._url, params={
                'query': query,
            }, json=post_data, timeout=self._timeout)

            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise ClickhouseError(e.response.text.strip()) from e

        try:
            return response.json()
        except ValueError:
            return str.strip(response.text)
