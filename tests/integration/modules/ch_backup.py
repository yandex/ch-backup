"""
Interface to ch-backup command-line tool.
"""

import datetime
import json
import os

import yaml

from . import docker, s3, utils

CH_BACKUP_CLI_PATH = '/usr/local/bin/ch-backup'
CH_BACKUP_CONF_PATH = '/etc/yandex/ch-backup/ch-backup.conf'
CBS_DEFAULT_JSON_INDENT = 4


class BackupManager:
    """
    Interface to ch-backup command-lime tool.
    """

    def __init__(self, context, node_name, config_path=None):
        self._container = docker.get_container(context, node_name)
        if config_path is None:
            config_path = CH_BACKUP_CONF_PATH
        self._config_path = config_path

        self._cmd_base = '{0} --protocol {1} --insecure  --config {2}'.format(
            CH_BACKUP_CLI_PATH, context.ch_backup['protocol'],
            self._config_path)

        self._s3_client = s3.S3Client(context)

    def backup(self, databases=None, tables=None, labels=None):
        """
        Perform backup.
        """
        options = []
        if databases:
            options.append('--databases {0}'.format(','.join(databases or [])))
        if tables:
            options.append('--tables {0}'.format(','.join(tables or [])))
        for key, value in (labels or {}).items():
            options.append('--label {0}={1}'.format(key, value))

        return self._exec('backup {0}'.format(' '.join(options)))

    def delete(self, backup_id):
        """
        Delete backup entry.
        """
        backup_id = self._normalize_id(backup_id)
        return self._exec('delete {0}'.format(backup_id))

    def purge(self):
        """
        Perform purge.
        """
        return self._exec('purge')

    def adjust_backup_ctime(self, backup_id, timedelta):
        """
        Move backup create time by timedelta
        """
        backup = self.get_backup(backup_id)
        date_fmt = backup.date_fmt
        for item in ('start_time', 'end_time'):
            dt_str = getattr(backup, item)
            dt = datetime.datetime.strptime(dt_str, date_fmt) \
                + datetime.timedelta(**timedelta)
            setattr(backup, item, dt.strftime(date_fmt))

        return self._s3_client.upload_data(backup.dump_json().encode('utf-8'),
                                           backup.path)

    def update_config(self, update):
        """
        Apply new config to old one
        """
        conf = yaml.load(
            self._container.exec_run(
                '/bin/cat {0}'.format(self._config_path),
                user='root').decode())

        utils.merge(conf, update)
        docker.put_file(
            self._container,
            yaml.dump(
                conf, default_flow_style=False, encoding='utf-8', indent=4),
            self._config_path)

    def get_missed_paths(self, backup_id):
        """
        Get backup entry metadata.
        """
        backup = self.get_backup(backup_id)
        missed = []
        for path in backup.get_file_paths():
            if not self._s3_client.path_exists(path):
                missed.append(path)
        return missed

    def get_backup_ids(self, list_all=True):
        """
        Get list of existing backup entries / identifiers.
        """
        cmd = 'list' if not list_all else 'list -a'
        output = self._exec(cmd)
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
        cmd = '{0} {1}'.format(self._cmd_base, command)
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
    def meta(self):
        """
        Backup meta.
        """
        return self._metadata.get('meta', {})

    @property
    def version(self):
        """
        ClickHouse version.
        """
        return self.meta.get('ch_version')

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

    @property
    def data_count(self):
        """
        The number of data parts (not deduplicated).
        """
        count = 0
        for _, db_contents in self._metadata['databases'].items():
            for _, table_contents in db_contents['parts_paths'].items():
                for _, part_contents in table_contents.items():
                    if part_contents['link'] is False:
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

    @start_time.setter
    def start_time(self, item):
        """
        Backup start time
        """
        self._metadata['meta']['start_time'] = item

    @property
    def end_time(self):
        """
        Backup end time
        """
        return self.meta.get('end_time')

    @end_time.setter
    def end_time(self, item):
        """
        Backup end time
        """
        self._metadata['meta']['end_time'] = item

    @property
    def date_fmt(self):
        """
        Backup date format
        """
        return self.meta.get('date_fmt')

    @property
    def path(self):
        """
        Path to backup struct
        """
        return os.path.join(self.meta.get('path'), 'backup_struct.json')

    def dump_json(self):
        """
        Dump struct to json data
        """
        return json.dumps(self._metadata, indent=CBS_DEFAULT_JSON_INDENT)

    def get_file_paths(self):
        """
        Return all storage paths
        """
        paths = set()
        for _, db_contents in self._metadata['databases'].items():
            for _, table_sql_path in db_contents['tables_sql_paths']:
                paths.add(table_sql_path)
            for _, table_contents in db_contents['parts_paths'].items():
                for _, part_contents in table_contents.items():
                    paths.update(part_contents['paths'])
        return tuple(paths)
