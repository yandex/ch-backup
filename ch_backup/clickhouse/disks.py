"""
Clickhouse-disks controls temporary cloud storage disks management.
"""

import logging
import os
from subprocess import PIPE, Popen
from typing import Dict, List, Optional
from urllib.parse import urlparse

import xmltodict

import ch_backup.logging as ch_logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.models import Disk, Table
from ch_backup.config import Config


class ClickHouseDisksException(RuntimeError):
    """
    ClickHouse-disks call error.
    """
    pass


class ClickHouseTemporaryDisks:
    """
    Manages temporary cloud storage disks.
    """
    def __init__(self, ch_ctl: ClickhouseCTL, backup_layout: BackupLayout, config: Config, backup_meta: BackupMetadata,
                 source_bucket: Optional[str], source_path: Optional[str], source_endpoint: Optional[str]):
        self._ch_ctl = ch_ctl
        self._backup_layout = backup_layout
        self._config = config['backup']
        self._config_dir = config['clickhouse']['config_dir']
        self._backup_meta = backup_meta

        self._source_bucket: str = source_bucket or ''
        self._source_path: str = source_path or ''
        self._source_endpoint: str = source_endpoint or ''
        if self._backup_meta.cloud_storage.enabled and source_bucket is None:
            raise RuntimeError('Backup contains cloud storage data, cloud-storage-source-bucket must be set.')

        self._disks: Dict[str, Disk] = {}
        self._created_disks: Dict[str, Disk] = {}

    def __enter__(self):
        self._disks = self._ch_ctl.get_disks()
        for disk_name in self._backup_meta.cloud_storage.disks:
            disk = self._disks[disk_name]
            self._create_temporary_disk(self._backup_meta, disk, self._source_bucket, self._source_path,
                                        self._source_endpoint)
        self._backup_layout.wait()
        return self

    def __exit__(self, exc_type, *args, **kwargs):
        if exc_type is not None:
            ch_logging.warning(f'Omitting tmp cloud storage disk cleanup due to exception: "{exc_type}"')
            return False

        for disk in self._created_disks.values():
            ch_logging.debug(f'Removing tmp disk {disk.name}')
            try:
                os.remove(_get_config_path(self._config_dir, disk.name))
                return True
            except FileNotFoundError:
                pass

    def _create_temporary_disk(self, backup_meta: BackupMetadata, disk: Disk, source_bucket: str, source_path: str,
                               source_endpoint: str) -> None:
        tmp_disk_name = _get_tmp_disk_name(disk.name)
        ch_logging.debug(f'Creating tmp disk {tmp_disk_name}')
        with open('/var/lib/clickhouse/preprocessed_configs/config.xml', 'r', encoding='utf-8') as f:
            config = xmltodict.parse(f.read())
            config = config.get('clickhouse', config.get('yandex'))
            disk_config = config['storage_configuration']['disks'][disk.name]

        endpoint = urlparse(disk_config['endpoint'])
        endpoint_netloc = source_endpoint or endpoint.netloc
        disk_config['endpoint'] = os.path.join(f'{endpoint.scheme}://{endpoint_netloc}', source_bucket, source_path,
                                               '')
        with open(_get_config_path(self._config_dir, tmp_disk_name), 'w', encoding='utf-8') as f:
            xmltodict.unparse({'yandex': {
                'storage_configuration': {
                    'disks': {
                        tmp_disk_name: disk_config,
                    },
                },
            }},
                              f,
                              pretty=True)

        self._ch_ctl.reload_config()
        source_disk = self._ch_ctl.get_disk(tmp_disk_name)
        ch_logging.debug(f'Restoring Cloud Storage "shadow" data  of disk "{disk.name}"')
        self._backup_layout.download_cloud_storage_metadata(backup_meta, source_disk, disk.name)
        self._created_disks[tmp_disk_name] = source_disk

    def copy_part(self, backup_meta: BackupMetadata, table: Table, part: PartMetadata) -> None:
        """
        Copy data from temporary cloud storage disk to actual.
        """
        target_disk = self._disks[part.disk_name]
        source_disk = self._created_disks[_get_tmp_disk_name(part.disk_name)]

        for path, disk in table.paths_with_disks:
            if disk.name == target_disk.name:
                table_path = os.path.relpath(path, target_disk.path)
                target_path = os.path.join(table_path, 'detached')
                source_path = os.path.join('shadow', backup_meta.get_sanitized_name(), table_path, part.name, '')
                _copy_dir(source_disk.name, source_path, target_disk.name, target_path)
                return
        raise Exception(f'Disk "{target_disk.name}" path not found for table `{table.database}`.`{table.name}`')


def _get_config_path(config_dir: str, disk_name: str) -> str:
    return os.path.join(config_dir, f'cloud_storage_tmp_disk_{disk_name}.xml')


def _get_tmp_disk_name(disk_name: str) -> str:
    return f'{disk_name}_source'


def _copy_dir(from_disk: str, from_path: str, to_disk: str, to_path: str) -> None:
    result = _exec('copy',
                   common_args=[],
                   command_args=['--diskFrom', from_disk, '--diskTo', to_disk, from_path, to_path])
    ch_logging.warning(f'clickhouse-disks copy result: {os.linesep.join(result)}')


def _exec(command: str, common_args: List[str], command_args: List[str]) -> List[str]:
    logger = logging.getLogger('clickhouse-disks')
    command_args = [
        '/usr/bin/clickhouse-disks',
        *common_args,
        command,
        *command_args,
    ]
    ch_logging.debug(f'Executing "{" ".join(command_args)}"')

    with Popen(command_args, stdout=PIPE, stderr=PIPE, shell=False) as proc:
        while proc.poll() is None:
            for line in proc.stderr.readlines():  # type: ignore
                logger.info(line.decode('utf-8').strip())
        if proc.returncode != 0:
            raise ClickHouseDisksException(f'clickhouse-disks call failed with exitcode: {proc.returncode}')

        return list(map(lambda b: b.decode('utf-8'), proc.stdout.readlines()))  # type: ignore
