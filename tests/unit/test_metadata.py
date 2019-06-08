"""
Unit tests for layout module.
"""

import json
from datetime import datetime

import pytest

from ch_backup.backup.metadata import BackupMetadata, BackupState


class TestBackupMetadata:
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
            'bytes': 0,
            'real_bytes': 0,
            'hostname': 'clickhouse01.test_net_711',
            'version': '1.0.100',
            'ch_version': '19.1.16',
            'labels': None,
            **meta,
        }
        metadata = {
            'meta': meta,
            'databases': [],
        }

        backup = BackupMetadata.load_json(json.dumps(metadata))
        assert backup.name == meta['name']
        assert backup.path == meta['path']
        assert backup.state == BackupState(meta['state'])
        time_format = meta['date_fmt']
        assert backup.time_format == time_format
        assert backup.start_time == datetime.strptime(meta['start_time'], time_format)
        end_time_str = meta['end_time']
        end_time = datetime.strptime(end_time_str, time_format) if end_time_str else None
        assert backup.end_time == end_time
        assert backup.size == meta['bytes']
        assert backup.real_size == meta['real_bytes']
        assert backup.hostname == meta['hostname']
        assert backup.version == meta['version']
        assert backup.ch_version == meta['ch_version']
        assert backup.labels == meta['labels']

    def test_dump_is_compact(self):
        backup = BackupMetadata(name='20181017T210300',
                                path='ch_backup/20181017T210300',
                                version='1.0.100',
                                ch_version='19.1.16',
                                time_format='%Y-%m-%dT%H:%M:%S%Z',
                                hostname='clickhouse01.test_net_711')

        assert backup.dump_json().find(' ') == -1
