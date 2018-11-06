"""
Unit tests for layout module.
"""

import json
from datetime import datetime

import pytest

from ch_backup.backup.layout import (ClickhouseBackupState,
                                     ClickhouseBackupStructure)


class TestClickhouseBackupStructure:
    """
    Tests for ClickhouseBackupStructure.
    """

    @pytest.mark.parametrize('meta', [
        {
            'state': 'created',
            'start_time': '2018-10-18 00:03:00 +0300',
            'end_time': '2018-10-18 00:04:00 +0300',
        },
        {
            'state': 'creating',
            'start_time': '2018-10-18 00:03:00 +0300',
            'end_time': None,
        },
    ])
    def test_load_json(self, meta):
        """
        load_json() test.
        """

        meta = {
            'name': '20181017T210300',
            'path': 'ch_backup/20181017T210300',
            'date_fmt': '%Y-%m-%d %H:%M:%S %z',
            'rows': 0,
            'real_rows': 0,
            'bytes': 0,
            'real_bytes': 0,
            'hostname': 'clickhouse01.test_net_711',
            'ch_version': 'v18.5.1-testing',
            **meta,
        }
        metadata = {
            'meta': meta,
            'databases': [],
        }

        backup = ClickhouseBackupStructure.load_json(json.dumps(metadata))
        assert backup.name == meta['name']
        assert backup.path == meta['path']
        assert backup.state == ClickhouseBackupState(meta['state'])
        date_fmt = meta['date_fmt']
        assert backup.date_fmt == date_fmt
        assert backup.start_time == datetime.strptime(meta['start_time'],
                                                      date_fmt)
        end_time_str = meta['end_time']
        end_time = datetime.strptime(end_time_str,
                                     date_fmt) if end_time_str else None
        assert backup.end_time == end_time
        assert backup.rows == meta['rows']
        assert backup.real_rows == meta['real_rows']
        assert backup.bytes == meta['bytes']
        assert backup.real_bytes == meta['real_bytes']
        assert backup.hostname == meta['hostname']
        assert backup.ch_version == meta['ch_version']
