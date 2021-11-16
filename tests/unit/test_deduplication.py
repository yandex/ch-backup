"""
Unit tests for deduplication module.
"""

from unittest.mock import MagicMock

from ch_backup.backup.deduplication import \
    collect_dedup_references_for_batch_backup_deletion
from ch_backup.backup.metadata import BackupMetadata
from tests.unit.utils import parametrize


@parametrize({
    'id': 'single data part',
    'args': {
        'retained_backups': [{
            'meta': {
                'name': 'backup2',
                'path': 'ch_backup/backup2',
                'state': 'created',
                'start_time': '2021-05-09 00:00:00 +0300',
                'end': '2021-05-09 00:01:00 +0300',
                'time_format': '%Y-%m-%d %H:%M:%S %z',
                'bytes': 1024,
                'real_bytes': 0,
                'labels': [],
                'version': '1.0.0',
                'ch_version': '21.3.2.1',
                'hostname': 'host1',
            },
            'databases': {
                'db1': {
                    'tables': {
                        'table1': {
                            'engine': 'MergeTree',
                            'parts': {
                                'part1': {
                                    'bytes': 1024,
                                    'files': ['file1', 'file2'],
                                    'checksum': 'checksum1',
                                    'tarball': True,
                                    'link': 'ch_backup/backup1',
                                    'disk_name': 'default',
                                },
                            },
                        },
                    },
                },
            },
        }],
        'deleting_backups': [{
            'meta': {
                'name': 'backup1',
                'path': 'ch_backup/backup1',
                'state': 'created',
                'start_time': '2021-05-08 00:00:00 +0300',
                'end': '2021-05-08 00:01:00 +0300',
                'time_format': '%Y-%m-%d %H:%M:%S %z',
                'bytes': 1024,
                'real_bytes': 1024,
                'labels': [],
                'version': '1.0.0',
                'ch_version': '21.3.2.1',
                'hostname': 'host1',
            },
            'databases': {
                'db1': {
                    'tables': {
                        'table1': {
                            'engine': 'MergeTree',
                            'parts': {
                                'part1': {
                                    'bytes': 1024,
                                    'files': ['file1', 'file2'],
                                    'checksum': 'checksum1',
                                    'tarball': True,
                                    'link': None,
                                    'disk_name': 'default',
                                },
                            },
                        },
                    },
                },
            },
        }],
        'result': {
            'backup1': {
                'db1': {
                    'table1': {'part1'},
                },
            },
        },
    },
})
def test_collect_dedup_references_for_batch_backup_deletion(retained_backups, deleting_backups, result):
    retained_backups = [BackupMetadata.load(raw_backup) for raw_backup in retained_backups]
    deleting_backups = [BackupMetadata.load(raw_backup) for raw_backup in deleting_backups]

    layout = MagicMock()
    layout.reload_backup = lambda backup, use_light_meta: backup

    assert collect_dedup_references_for_batch_backup_deletion(
        layout=layout,
        retained_backups_with_light_meta=retained_backups,
        deleting_backups_with_light_meta=deleting_backups) == result
