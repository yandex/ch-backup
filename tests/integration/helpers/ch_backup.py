"""
Interface to ch-backup command-line tool.
"""

import json

from . import docker

CH_BACKUP_CLI_PATH = '/usr/local/bin/ch-backup'


class BackupManager:
    """
    Interface to ch-backup command-lime tool.
    """

    def __init__(self, context, node_name):
        self._container = docker.get_container(context, node_name)

    def backup(self):
        """
        Perform backup.
        """
        return self._exec('backup')

    def get_backup_ids(self):
        """
        Get list of existing backup entries / identifiers.
        """
        output = self._exec('list')
        return list(filter(None, output.split('\n')))

    def get_backup(self, backup_id):
        """
        Get backup entry metadata.
        """
        backup_id = self._normalize_id(backup_id)
        output = self._exec('show {0}'.format(backup_id))
        return Backup(json.loads(output))

    def restore(self, backup_id, schema_only=False):
        """
        Restore backup entry.
        """
        backup_id = self._normalize_id(backup_id)
        options = []
        if schema_only:
            options.append('--schema-only')
        return self._exec('restore {0} {1}'.format(' '.join(options),
                                                   backup_id))

    def _exec(self, command):
        cmd = '{0} {1}'.format(CH_BACKUP_CLI_PATH, command)
        return self._container.exec_run(cmd, user='root').decode()

    def _normalize_id(self, backup_id):
        if isinstance(backup_id, int):
            return self.get_backup_ids()[backup_id]
        return backup_id


class Backup:
    """
    Class representing backup metadata.
    """

    def __init__(self, metadata):
        self._metadata = metadata

    @property
    def version(self):
        """
        ClickHouse version.
        """
        return self._metadata.get('meta', {}).get('ch_version')

    @property
    def link_count(self):
        """
        The number of links (deduplicated parts).
        """
        count = 0
        for _, db_contents in self._metadata['databases'].items():
            for _, table_contents in db_contents['parts_paths'].items():
                for _, part_contents in table_contents.items():
                    if part_contents['link']:
                        count += 1
        return count
