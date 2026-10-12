"""
Backup layout over the schema of a live replica.
"""

import re

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, TableMetadata
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.config import Config
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.version import get_version

MIN_CLICKHOUSE_VERSION = "23.8"
MASKED_VALUE = "[HIDDEN]"
SOURCE_SETTINGS = {
    "format_display_secrets_in_show_and_select": 1,
    "show_table_uuid_in_table_create_query_if_not_nil": 1,
}
_LITERAL = r"'(?:[^'\\]|\\.)*'"
REPLICA_ARGUMENT_RE = re.compile(
    rf"\bReplicated\w*MergeTree\(\s*{_LITERAL}\s*,\s*(?P<table>{_LITERAL})"
    rf"|\bENGINE\s*=\s*Replicated\(\s*{_LITERAL}\s*,\s*{_LITERAL}\s*,\s*(?P<db>{_LITERAL})"
)


class ReplicaLayout(BackupLayout):
    """
    Backup layout that takes the schema from a live replica of the shard instead of a backup.

    Restore reads databases, tables and their create statements only through get_backup and
    the get_*_create_statements methods. get_backup reads and checks the whole schema, so a
    wrong source or an unusable statement fails before the restore changes the host.

    Tables of Replicated databases are left out: ClickHouse creates them from ZooKeeper.
    The source is queried with SOURCE_SETTINGS, so create statements come with UUIDs, which
    ZooKeeper paths may depend on, and with secrets, if the source enables
    display_secrets_in_show_and_select and grants displaySecretsInShowAndSelect.
    """

    def __init__(
        self,
        config: Config,
        local_ctl: ClickhouseCTL,
        source_ctl: ClickhouseCTL,
        replica_name: str | None,
    ) -> None:
        super().__init__(config)
        self._zookeeper_hosts = config["zookeeper"].get("hosts")
        self._local_ctl = local_ctl
        self._source_ctl = source_ctl
        self._replica_name = replica_name
        self._database_statements: dict[str, str] = {}
        self._table_statements: dict[tuple[str, str], str] = {}

    def get_backup(
        self, backup_name: str, use_light_meta: bool = False
    ) -> BackupMetadata:
        """
        Read the schema of the replica and describe it as a backup without data.
        """
        self._check_source(backup_name)
        backup = BackupMetadata(
            name=backup_name,
            version=get_version(),
            ch_version=self._source_ctl.get_version(),
            time_format=self._config["time_format"],
            hostname=backup_name,
            schema_only=True,
        )
        for db in self._source_ctl.get_databases(self._config["exclude_dbs"]):
            self._database_statements[db.name] = self._check_statement(
                backup_name,
                db.name,
                self._source_ctl.get_database_schema(db.name),
            )
            backup.add_database(db)
            if db.is_external_db_engine() or db.is_replicated_db_engine():
                continue
            for table in self._source_ctl.get_tables(db.name):
                if not table.metadata_path:
                    logging.warning(
                        'Skipping table "{}"."{}": it has no metadata on {}',
                        db.name,
                        table.name,
                        backup_name,
                    )
                    continue
                self._table_statements[(db.name, table.name)] = self._check_statement(
                    backup_name, f"{db.name}.{table.name}", table.create_statement
                )
                backup.add_table(
                    TableMetadata(db.name, table.name, table.engine, table.uuid)
                )
        logging.info(
            "Read schema of {} databases and {} tables from {}",
            len(self._database_statements),
            len(self._table_statements),
            backup_name,
        )
        return backup

    def get_database_create_statements(
        self, backup_meta: BackupMetadata, db_names: list[str]
    ) -> list[tuple[str, str]]:
        """
        Return create statements of the databases read from the replica.
        """
        return [(name, self._database_statements[name]) for name in db_names]

    def get_table_create_statements(
        self, backup_meta: BackupMetadata, db_name: str, table_names: list[str]
    ) -> list[tuple[str, str]]:
        """
        Return create statements of the tables read from the replica.
        """
        return [(name, self._table_statements[(db_name, name)]) for name in table_names]

    def _check_source(self, source_host: str) -> None:
        """
        Fail unless the source is another replica of the same shard on a supported version.

        Macros other than replica must match, so the schema cannot come from another shard
        or cluster. Versions must match up to major.minor: create statements of a newer
        version may not parse on an older one. ZooKeeper must be configured, as the replica
        left there by the previous installation of the host is removed before the restore,
        and the name to remove must not be the one of the source.
        """
        for ctl in (self._local_ctl, self._source_ctl):
            if not ctl.ch_version_ge(MIN_CLICKHOUSE_VERSION):
                raise ClickhouseBackupError(
                    f"Restore from a replica needs ClickHouse {MIN_CLICKHOUSE_VERSION} or newer, "
                    f"got {ctl.get_version()}"
                )
        local_version = self._local_ctl.get_version()
        source_version = self._source_ctl.get_version()
        if local_version.split(".")[:2] != source_version.split(".")[:2]:
            raise ClickhouseBackupError(
                f"ClickHouse {local_version} on this host differs from {source_version} on {source_host}"
            )
        if local_version != source_version:
            logging.warning(
                "ClickHouse {} on this host differs from {} on {} in the patch version",
                local_version,
                source_version,
                source_host,
            )

        local_macros = self._local_ctl.get_macros()
        source_macros = self._source_ctl.get_macros()
        local_replica = local_macros.pop("replica", None)
        source_replica = source_macros.pop("replica", None)
        if not local_replica or not source_replica:
            raise ClickhouseBackupError(
                "Cannot tell replicas apart: the replica macro is not set"
            )
        if local_replica == source_replica:
            raise ClickhouseBackupError(
                f"{source_host} is this replica itself: {source_replica}"
            )
        if local_macros != source_macros:
            raise ClickhouseBackupError(
                f"{source_host} is not a replica of the same shard: macros {source_macros} "
                f"differ from {local_macros}"
            )
        if not self._zookeeper_hosts:
            raise ClickhouseBackupError(
                "Restore from a replica needs ZooKeeper in the config to remove the replica "
                "of the previous installation"
            )
        if self._replica_name == source_replica:
            raise ClickhouseBackupError(
                f"Replica name {self._replica_name} to remove is the one of {source_host}"
            )

    @staticmethod
    def _check_statement(source_host: str, name: str, statement: str) -> str:
        """
        Fail if a create statement read from the replica cannot be used on this host.

        A statement with secrets hidden by ClickHouse makes an unusable object. A replica
        name given as a literal instead of a {replica} macro is the one of the source, so
        this host would take over the replica of the source in ZooKeeper.
        """
        if MASKED_VALUE in statement:
            raise ClickhouseBackupError(
                f"Create statement of {name} on {source_host} has hidden secrets: enable "
                "display_secrets_in_show_and_select and grant displaySecretsInShowAndSelect there"
            )
        for match in REPLICA_ARGUMENT_RE.finditer(statement):
            replica = match.group("table") or match.group("db")
            if "{replica}" not in replica:
                raise ClickhouseBackupError(
                    f"Create statement of {name} on {source_host} has replica name {replica} "
                    "without the {replica} macro"
                )
        return statement
