"""
Interface to ch-backup command-line tool.
"""

import json
import os
from typing import Sequence, Set, Union

import yaml

from . import docker, s3, utils
from .typing import ContextT

CH_BACKUP_CLI_PATH = '/usr/local/bin/ch-backup'
CH_BACKUP_CONF_PATH = '/etc/yandex/ch-backup/ch-backup.conf'
CBS_DEFAULT_JSON_INDENT = 4

BackupId = Union[int, str]


class Backup:
    """
    Class representing backup metadata.
    """

    def __init__(self, metadata: dict) -> None:
        self._metadata = metadata

    @property
    def meta(self) -> dict:
        """
        Backup meta.
        """
        return self._metadata.get('meta', {})

    @property
    def link_count(self) -> int:
        """
        The number of links (deduplicated parts).
        """
        count = 0
        for db_obj in self._metadata['databases'].values():
            for table_obj in db_obj['tables'].values():
                for part_obj in table_obj['parts'].values():
                    if part_obj['link']:
                        count += 1
        return count

    @property
    def data_count(self) -> int:
        """
        The number of data parts (not deduplicated).
        """
        count = 0
        for db_obj in self._metadata['databases'].values():
            for table_obj in db_obj['tables'].values():
                for part_obj in table_obj['parts'].values():
                    if not part_obj['link']:
                        count += 1
        return count

    @property
    def name(self):
        """
        Backup name.
        """
        return self.meta.get('name')

    @property
    def state(self):
        """
        Backup state.
        """
        return self.meta.get('state')

    @property
    def start_time(self):
        """
        Backup start time
        """
        return self.meta.get('start_time')

    @property
    def end_time(self):
        """
        Backup end time
        """
        return self.meta.get('end_time')

    @property
    def date_fmt(self):
        """
        Backup date format
        """
        return self.meta.get('date_fmt')

    @property
    def path(self) -> str:
        """
        Path to backup struct
        """
        return os.path.join(self.meta['path'], 'backup_struct.json')

    def update(self, metadata: dict, merge: bool = True) -> None:
        """
        Update metadata.
        """
        if merge:
            utils.merge(self._metadata, metadata)
        else:
            self._metadata = metadata

    def dump_json(self) -> str:
        """
        Dump struct to json data
        """
        return json.dumps(self._metadata, indent=CBS_DEFAULT_JSON_INDENT)

    def get_file_paths(self) -> Sequence[str]:
        """
        Return all storage paths
        """
        backup_path = self.meta['path']
        file_paths: Set[str] = set()
        for db_name, db_obj in self._metadata['databases'].items():
            for table_name, table_obj in db_obj['tables'].items():
                for part_name, part_obj in table_obj['parts'].items():
                    part_path = os.path.join(
                        part_obj.get('link') or backup_path, 'data', db_name, table_name, part_name)
                    file_paths.update(os.path.join(part_path, f) for f in part_obj['files'])

        return tuple(file_paths)


class BackupManager:
    """
    Interface to ch-backup command-lime tool.
    """

    def __init__(self, context: ContextT, node_name: str) -> None:
        self._container = docker.get_container(context, node_name)
        self._s3_client = s3.S3Client(context)
        self._config_path = CH_BACKUP_CONF_PATH
        self._cmd_base = '{0} --protocol {1} --insecure  --config {2}'.format(
            CH_BACKUP_CLI_PATH, context.ch_backup['protocol'], self._config_path)

    def backup(self,
               name: str = None,
               force: bool = None,
               databases: Sequence[str] = None,
               tables: Sequence[str] = None,
               labels: dict = None) -> str:
        """
        Execute backup command.
        """
        options = []
        if name:
            options.append('--name {0}'.format(name))
        if force:
            options.append('--force')
        if databases:
            options.append('--databases {0}'.format(','.join(databases or [])))
        if tables:
            options.append('--tables {0}'.format(','.join(tables or [])))
        for key, value in (labels or {}).items():
            options.append('--label {0}={1}'.format(key, value))

        return self._exec('backup {0}'.format(' '.join(options))).strip()

    def delete(self, backup_id: BackupId) -> str:
        """
        Execute delete command.
        """
        backup_id = self._normalize_id(backup_id)
        return self._exec('delete {0}'.format(backup_id))

    def purge(self) -> str:
        """
        Execute purge command.
        """
        return self._exec('purge')

    def version(self) -> str:
        """
        Execute version command.
        """
        return self._exec('version')

    def update_backup_metadata(self, backup_id: BackupId, metadata: dict, merge: bool = True) -> None:
        """
        Update backup metadata.
        """
        backup = self.get_backup(backup_id)
        backup.update(metadata, merge)
        self._s3_client.upload_data(backup.dump_json().encode('utf-8'), backup.path)

    def update_config(self, update: dict) -> None:
        """
        Apply new config to old one
        """
        output = self._container.exec_run(f'/bin/cat {self._config_path}', user='root').output.decode()
        conf = yaml.load(output, yaml.SafeLoader)

        utils.merge(conf, update)
        docker.put_file(self._container, yaml.dump(conf, default_flow_style=False, encoding='utf-8', indent=4),
                        self._config_path)

    def get_missed_paths(self, backup_id: BackupId) -> Sequence[str]:
        """
        Get backup entry metadata.
        """
        backup = self.get_backup(backup_id)
        missed = []
        for path in backup.get_file_paths():
            if not self._s3_client.path_exists(path):
                missed.append(path)
        return missed

    def get_backup_ids(self, list_all: bool = True) -> Sequence[str]:
        """
        Get list of existing backup entries / identifiers.
        """
        cmd = 'list' if not list_all else 'list -a'
        output = self._exec(cmd)
        return list(filter(None, output.split('\n')))

    def get_backup(self, backup_id: BackupId) -> Backup:
        """
        Get backup entry metadata.
        """
        backup_id = self._normalize_id(backup_id)
        output = self._exec('show {0}'.format(backup_id))
        return Backup(json.loads(output))

    def restore(self, backup_id: BackupId, schema_only: bool = False) -> str:
        """
        Restore backup entry.
        """
        backup_id = self._normalize_id(backup_id)
        options = []
        if schema_only:
            options.append('--schema-only')
        return self._exec('restore {0} {1}'.format(' '.join(options), backup_id))

    def _exec(self, command: str) -> str:
        cmd = '{0} {1}'.format(self._cmd_base, command)
        result = self._container.exec_run(cmd, user='root')
        return result.output.decode().strip()

    def _normalize_id(self, backup_id: BackupId) -> str:
        if isinstance(backup_id, int):
            return self.get_backup_ids()[backup_id]
        return backup_id


def get_version() -> str:
    """
    Get ch-backup version.
    """
    with open('ch_backup/version.txt') as f:
        return f.read().strip()
