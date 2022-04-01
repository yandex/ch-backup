"""
Testing utilities.
"""
from datetime import timedelta
from typing import List

import pytest

from ch_backup.backup.deduplication import PartDedupInfo
from ch_backup.backup.metadata import BackupMetadata, BackupState
from ch_backup.util import utcnow


def parametrize(*tests):
    """
    A wrapper for `pytest.mark.parametrize` that eliminates parallel lists in the interface.

    Example:
    ```
    @parametrize(
        {
            'id': 'test1',
            'args': {
                'arg1': 'value1-1',
                'arg2': 'value1-2',
            }
        },
        {
            'id': 'test2',
            'args': {
                'arg1': 'value2-1',
                'arg2': 'value2-2',
            }
        }
    )
    ```
    It equals to:
    ```
    @pytest.mark.parametrize(
        ids=['test1', 'test2'],
        argnames=['arg1', 'arg2'],
        argvalues=[
            (
                'value1-1',
                'value1-2',
            ),
            (
                'value2-1',
                'value2-2',
            ),
        ]
    )
    ```
    """
    ids: List[str] = []
    argnames: List[str] = []
    argvalues: list = []
    for test in tests:
        ids.append(test['id'])

        test_args = sorted(test['args'].items())
        test_argnames = [arg[0] for arg in test_args]
        test_argvalues = [arg[1] for arg in test_args]

        if not argnames:
            argnames = test_argnames
        else:
            assert argnames == test_argnames

        argvalues.append(test_argvalues)

    return pytest.mark.parametrize(ids=ids, argnames=argnames, argvalues=argvalues)


def backup_metadata(name: str,
                    state: BackupState,
                    *,
                    hostname: str = 'host1',
                    age: timedelta = timedelta(minutes=1),
                    schema_only: bool = False,
                    databases: dict = None) -> BackupMetadata:
    """
    Build and return backup metadata.
    """
    if databases is None:
        databases = {}

    backup_size = 0
    real_backup_size = 0
    for db in databases.values():
        for table in db['tables'].values():
            for part in table['parts'].values():
                part_size = part['bytes']
                backup_size += part_size
                if part['link']:
                    real_backup_size += part_size

    time_format = '%Y-%m-%d %H:%M:%S %z'
    current_time = utcnow()
    start_time = (current_time - age).strftime(time_format)
    end_time = None
    if state not in (BackupState.CREATING, BackupState.DELETING):
        end_time = (current_time - age + timedelta(minutes=1)).strftime(time_format)

    return BackupMetadata.load({
        'meta': {
            'name': name,
            'path': f'ch_backup/{name}',
            'hostname': hostname,
            'state': state,
            'start_time': start_time,
            'end_time': end_time,
            'time_format': time_format,
            'bytes': backup_size,
            'real_bytes': real_backup_size,
            'labels': [],
            'version': '1.0.0',
            'ch_version': '21.3.2.1',
            'schema_only': schema_only,
        },
        'databases': databases,
    })


def parts(count: int, link: str = None) -> dict:
    """
    Build and return parts metadata.
    """
    result = {}
    for n in range(1, count + 1):
        result[f'part{n}'] = {
            'bytes': 1024,
            'files': ['file1', 'file2'],
            'checksum': 'checksum1',
            'tarball': True,
            'link': link,
            'disk_name': 'default',
        }

    return result


def parts_dedup_info(backup_path: str, count: int, verified: bool = False) -> dict:
    """
    Build and return parts deduplication info.
    """
    result = {}
    for name, part in parts(count).items():
        result[name] = PartDedupInfo(backup_path=backup_path,
                                     checksum=part['checksum'],
                                     size=part['bytes'],
                                     files=part['files'],
                                     tarball=True,
                                     disk_name=part['disk_name'],
                                     verified=verified)

    return result
