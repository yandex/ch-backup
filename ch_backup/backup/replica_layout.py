"""
Backup layout over a live replica.
"""

import re

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, TableMetadata
from ch_backup.backup.sources import BackupSources
from ch_backup.clickhouse.client import ClickhouseError
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.config import Config
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.version import get_version

MIN_CLICKHOUSE_VERSION = "23.8"
NAMED_COLLECTIONS_MIN_CLICKHOUSE_VERSION = "25.5"
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
HIDDEN_PASSWORD_RE = re.compile(
    r"\b(?:plaintext|sha256|double_sha1|bcrypt|scram_sha256)_password\b(?!\s+BY\b)"
)
CREATE_ACCESS_ENTITY_RE = re.compile(
    r"^(CREATE (?:USER|ROLE|SETTINGS PROFILE|QUOTA|ROW POLICY)) (\S+)"
)


class ReplicaLayout(BackupLayout):
    """
    Backup layout that takes the schema, functions, named collections and access entities
    from a live replica of the shard instead of a backup.

    Restore reads them only through get_backup, the get_*_create_statement(s) methods and
    restore_access. get_backup reads and checks everything requested, so a wrong source or
    an unusable statement fails before the restore changes the host.

    Tables of Replicated databases are left out: ClickHouse creates them from ZooKeeper.
    Named collections are taken only if this host does not have them, and access entities
    only from local storages, so objects shared through ZooKeeper are not touched.
    The source is queried with SOURCE_SETTINGS, so create statements come with UUIDs, which
    ZooKeeper paths may depend on, and with secrets, if the source enables
    display_secrets_in_show_and_select and grants displaySecretsInShowAndSelect.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(
        self,
        config: Config,
        local_ctl: ClickhouseCTL,
        source_ctl: ClickhouseCTL,
        sources: BackupSources,
        replica_name: str | None,
    ) -> None:
        super().__init__(config)
        self._zookeeper_hosts = config["zookeeper"].get("hosts")
        self._local_ctl = local_ctl
        self._source_ctl = source_ctl
        self._sources = sources
        self._replica_name = replica_name
        self._database_statements: dict[str, str] = {}
        self._table_statements: dict[tuple[str, str], str] = {}
        self._udf_statements: dict[str, str] = {}
        self._named_collection_statements: dict[str, str] = {}
        self._access_statements: list[str] = []

    def get_backup(
        self, backup_name: str, use_light_meta: bool = False
    ) -> BackupMetadata:
        """
        Read the requested objects of the replica and describe them as a backup without data.
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
        if self._sources.schema:
            self._read_schema(backup_name, backup)
        if self._sources.udf:
            self._read_udf(backup_name, backup)
        if self._sources.named_collections:
            self._read_named_collections(backup_name, backup)
        if self._sources.access:
            self._read_access(backup_name)
        logging.info(
            "Read {} databases, {} tables, {} functions, {} named collections and {} "
            "access statements from {}",
            len(self._database_statements),
            len(self._table_statements),
            len(self._udf_statements),
            len(self._named_collection_statements),
            len(self._access_statements),
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

    def get_udf_create_statement(
        self, backup_meta: BackupMetadata, filename: str
    ) -> str:
        """
        Return the create statement of a function read from the replica.
        """
        return self._udf_statements[filename]

    def get_named_collection_create_statement(
        self, backup_meta: BackupMetadata, filename: str
    ) -> str:
        """
        Return the create statement of a named collection read from the replica.
        """
        return self._named_collection_statements[filename]

    def restore_access(self, ch_ctl: ClickhouseCTL, replicated_access: bool) -> None:
        """
        Create the access entities of the replica on this host.

        Entities refer to each other in any direction, for example a settings profile
        applied to a user who refers to the profile. So the statements that fail are run
        again while each pass runs at least one of them.
        """
        if self._access_statements and replicated_access:
            raise ClickhouseBackupError(
                "This host keeps access entities in ZooKeeper, so local access entities "
                "of the replica cannot be restored"
            )
        pending = self._access_statements
        error: ClickhouseError | None = None
        while pending:
            failed = []
            for statement in pending:
                try:
                    ch_ctl.restore_access_entity(statement)
                except ClickhouseError as e:
                    error = e
                    failed.append(statement)
            if len(failed) == len(pending):
                raise ClickhouseBackupError(
                    f"Cannot restore {len(failed)} access statements from the replica"
                ) from error
            pending = failed

    def _read_schema(self, source_host: str, backup: BackupMetadata) -> None:
        """
        Read databases and tables of the replica.
        """
        for db in self._source_ctl.get_databases(self._config["exclude_dbs"]):
            self._database_statements[db.name] = self._check_statement(
                source_host, db.name, self._source_ctl.get_database_schema(db.name)
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
                        source_host,
                    )
                    continue
                self._table_statements[(db.name, table.name)] = self._check_statement(
                    source_host, f"{db.name}.{table.name}", table.create_statement
                )
                backup.add_table(
                    TableMetadata(db.name, table.name, table.engine, table.uuid)
                )

    def _read_udf(self, source_host: str, backup: BackupMetadata) -> None:
        """
        Read user defined functions of the replica.
        """
        for name, statement in self._source_ctl.get_udf_query().items():
            self._udf_statements[name] = self._check_statement(
                source_host, f"function {name}", statement
            )
            backup.add_udf(name)

    def _read_named_collections(self, source_host: str, backup: BackupMetadata) -> None:
        """
        Read named collections of the replica that this host does not have.

        Collections from the server config and ones shared through ZooKeeper are already
        here. ClickHouse shows create statements of collections only since 25.5.
        """
        missing = sorted(
            set(self._source_ctl.get_named_collections_query())
            - set(self._local_ctl.get_named_collections_query())
        )
        if not missing:
            return
        if not self._source_ctl.ch_version_ge(NAMED_COLLECTIONS_MIN_CLICKHOUSE_VERSION):
            raise ClickhouseBackupError(
                f"Named collections {', '.join(missing)} can be restored from a replica only "
                f"on ClickHouse {NAMED_COLLECTIONS_MIN_CLICKHOUSE_VERSION} or newer. "
                "Restore without them with --schema --access --udf"
            )
        statements = self._source_ctl.get_named_collections_create_queries()
        for name in missing:
            self._named_collection_statements[name] = self._check_statement(
                source_host, f"named collection {name}", statements[name]
            )
            backup.add_named_collection(name)

    def _read_access(self, source_host: str) -> None:
        """
        Read access entities of local storages of the replica and their grants.

        Without secrets shown ClickHouse prints the type of a password but not its hash, so
        such a user would be created without the password. Statements create entities with
        OR REPLACE to allow running the restore again after a failure.
        """
        for statement in self._source_ctl.get_local_access_statements():
            entity = CREATE_ACCESS_ENTITY_RE.match(statement)
            name = entity.group(0) if entity else "a grant"
            if MASKED_VALUE in statement or HIDDEN_PASSWORD_RE.search(statement):
                raise ClickhouseBackupError(
                    f"{name} on {source_host} has hidden secrets: enable "
                    "display_secrets_in_show_and_select and grant displaySecretsInShowAndSelect there"
                )
            self._access_statements.append(
                CREATE_ACCESS_ENTITY_RE.sub(r"\1 OR REPLACE \2", statement)
            )

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
