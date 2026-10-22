"""
Backup layout over a live replica.
"""

import re

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, TableMetadata
from ch_backup.backup.sources import BackupSources
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.schema import rewrite_database_schema, rewrite_table_schema
from ch_backup.config import Config
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.named_collections import NamedCollectionsStorageConfig
from ch_backup.logic.workload_entities import get_config_workload_entity_names
from ch_backup.version import get_version

MIN_CLICKHOUSE_VERSION = "23.8"
NAMED_COLLECTIONS_MIN_CLICKHOUSE_VERSION = "25.5"
WORKLOAD_ENTITIES_MIN_CLICKHOUSE_VERSION = "24.11"
SOURCE_SETTINGS = {
    "format_display_secrets_in_show_and_select": 1,
    "show_table_uuid_in_table_create_query_if_not_nil": 1,
}
ALLOWED_ACCESS_STORAGE_TYPES = {"users_xml", "ldap", "local_directory"}
_LITERAL = r"'(?:[^'\\]|\\.)*'"
REPLICA_ARGUMENT_RE = re.compile(
    rf"\bReplicated\w*MergeTree\(\s*{_LITERAL}\s*,\s*(?P<table>{_LITERAL})"
    rf"|\bENGINE\s*=\s*Replicated\(\s*{_LITERAL}\s*,\s*{_LITERAL}\s*,\s*(?P<db>{_LITERAL})"
)
HIDDEN_PASSWORD_RE = re.compile(
    r"\b(?:plaintext|sha256|double_sha1|bcrypt|scram_sha256)_password\b(?!\s+BY\b)"
)
SECRETS_HINT = (
    "enable display_secrets_in_show_and_select and grant displaySecretsInShowAndSelect"
)


class ReplicaLayout(BackupLayout):
    """
    Backup layout that reads what restore needs from a live replica of the shard.

    get_backup reads and checks everything requested before the restore changes the host.
    Objects shared through ZooKeeper or from the server config are left out: tables of
    Replicated databases, named collections and workload entities from the server config
    of this host or shared through ZooKeeper, access entities of non-local storages.
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
        self._ch_ctl_conf = config["clickhouse"]
        self._ch_config = ClickhouseConfig(config)
        self._local_ctl = local_ctl
        self._source_ctl = source_ctl
        self._sources = sources
        self._replica_name = replica_name
        self._host = ""
        self._database_statements: dict[str, str] = {}
        self._table_statements: dict[tuple[str, str], str] = {}
        self._udf_statements: dict[str, str] = {}
        self._named_collection_statements: dict[str, str] = {}
        self._workload_entity_statements: dict[str, str] = {}
        self._access_statements: list[str] = []

    def get_backup(
        self, backup_name: str, use_light_meta: bool = False
    ) -> BackupMetadata:
        """
        Read the requested objects of the replica as a backup without data.
        """
        self._host = backup_name
        self._check_source()
        backup = BackupMetadata(
            name=backup_name,
            version=get_version(),
            ch_version=self._source_ctl.get_version(),
            time_format=self._config["time_format"],
            hostname=backup_name,
            schema_only=True,
        )
        if self._sources.schema:
            self._read_schema(backup)
        if self._sources.udf:
            self._read_udf(backup)
        if self._sources.named_collections:
            self._read_named_collections(backup)
        if self._sources.workload_entities:
            self._read_workload_entities(backup)
        if self._sources.access:
            self._read_access()
        logging.info(
            "Read {} tables and {} access statements from {}",
            len(self._table_statements),
            len(self._access_statements),
            backup_name,
        )
        return backup

    def get_database_create_statements(
        self, backup_meta: BackupMetadata, db_names: list[str]
    ) -> list[tuple[str, str]]:
        """
        Return create statements of databases.
        """
        return [(name, self._database_statements[name]) for name in db_names]

    def get_table_create_statements(
        self,
        backup_meta: BackupMetadata,
        db_name: str,
        table_names: list[str],
        *,
        keep_going: bool = False,
    ) -> list[tuple[str, str]]:
        """
        Return create statements of tables.
        """
        return [(name, self._table_statements[(db_name, name)]) for name in table_names]

    def get_udf_create_statement(
        self, backup_meta: BackupMetadata, filename: str
    ) -> str:
        """
        Return the create statement of a function.
        """
        return self._udf_statements[filename]

    def get_named_collection_create_statement(
        self, backup_meta: BackupMetadata, filename: str
    ) -> str:
        """
        Return the create statement of a named collection.
        """
        return self._named_collection_statements[filename]

    def get_workload_entity_create_statement(
        self, backup_meta: BackupMetadata, filename: str
    ) -> str:
        """
        Return the create statement of a workload or a resource.
        """
        return self._workload_entity_statements[filename]

    def get_access_statements(self) -> list[str]:
        """
        Return statements that recreate access entities.
        """
        return self._access_statements

    def _read_schema(self, backup: BackupMetadata) -> None:
        """
        Read databases and tables, skipping tables without metadata as a backup does.
        Statements are checked as restore rewrites them with override_replica_name.
        """
        override = self._config["override_replica_name"]
        for db in self._source_ctl.get_databases(self._config["exclude_dbs"]):
            statement = self._source_ctl.get_database_schema(db.name)
            self._check(
                db.name,
                rewrite_database_schema(db, statement, override_replica_name=override),
            )
            self._database_statements[db.name] = statement
            backup.add_database(db)
            if db.is_external_db_engine() or db.is_replicated_db_engine():
                continue
            for table in self._source_ctl.get_tables(db.name):
                if not table.metadata_path:
                    logging.warning(f"Skipping {db.name}.{table.name}: no metadata")
                    continue
                self._table_statements[(db.name, table.name)] = table.create_statement
                rewrite_table_schema(table, override_replica_name=override)
                self._check(f"{db.name}.{table.name}", table.create_statement)
                backup.add_table(
                    TableMetadata(db.name, table.name, table.engine, table.uuid)
                )

    def _read_udf(self, backup: BackupMetadata) -> None:
        """
        Read functions. Restore skips ones this host has the same and replaces others.
        ClickHouse keeps them in ZooKeeper if user_defined_zookeeper_path is set, then they
        are shared with the replica and only missing ones are read: restore would drop a
        function that differs in format on all replicas.
        """
        local = {}
        if "user_defined_zookeeper_path" in self._ch_config.config:
            local = self._local_ctl.get_udf_query()
        for name, statement in self._source_ctl.get_udf_query().items():
            if name not in local:
                self._udf_statements[name] = self._check(name, statement)
                backup.add_udf(name)

    def _read_named_collections(self, backup: BackupMetadata) -> None:
        """
        Read named collections, except ones from the server config of this host. Unless
        they are in local storage, only missing ones are read: ones in ZooKeeper are shared
        with the replica, and restore may find them different in format and drop them on
        all replicas. Their create statements are shown since 25.5.
        """
        local = self._local_ctl.get_named_collections_query()
        missing = sorted(
            set(self._source_ctl.get_named_collections_query()) - set(local)
        )
        if not self._source_ctl.ch_version_ge(NAMED_COLLECTIONS_MIN_CLICKHOUSE_VERSION):
            if not missing:
                return
            raise ClickhouseBackupError(
                f"Named collections {', '.join(missing)} are missing on this host, and "
                f"restoring them needs ClickHouse {NAMED_COLLECTIONS_MIN_CLICKHOUSE_VERSION} "
                "or newer. Run with --schema --access --udf --workload to skip them"
            )
        statements = self._source_ctl.get_named_collections_create_queries()
        names = missing
        if NamedCollectionsStorageConfig.from_ch_config(
            self._ch_ctl_conf, self._ch_config
        ).is_local_storage():
            from_config = set(local) - set(
                self._local_ctl.get_named_collections_create_queries()
            )
            names = sorted(set(statements).union(missing) - from_config)
        for name in names:
            if name not in statements:
                raise ClickhouseBackupError(
                    f"Named collection {name} comes from the server config of {self._host} "
                    "and is missing in the server config of this host"
                )
            self._named_collection_statements[name] = self._check(
                name, statements[name]
            )
            backup.add_named_collection(name)

    def _read_workload_entities(self, backup: BackupMetadata) -> None:
        """
        Read workloads and resources, except ones from the server config of this host,
        failing on ones this host has of another type. ClickHouse keeps them in ZooKeeper
        if workload_zookeeper_path is set, then they are shared with the replica and only
        missing ones are read.
        """
        if not self._source_ctl.ch_version_ge(WORKLOAD_ENTITIES_MIN_CLICKHOUSE_VERSION):
            return
        local = self._local_ctl.get_workload_entities()
        from_config = get_config_workload_entity_names(self._ch_config)
        shared = "workload_zookeeper_path" in self._ch_config.config
        source = self._source_ctl.get_workload_entities()
        for name, (entity_type, statement) in source.items():
            if name in from_config or (shared and name in local):
                continue
            if name in local:
                local_type = local[name][0]
                if local_type != entity_type:
                    raise ClickhouseBackupError(
                        f"{name} is a {entity_type.value} on {self._host} and a "
                        f"{local_type.value} on this host"
                    )
            self._workload_entity_statements[name] = statement
            backup.add_workload_entity(name)

    def _read_access(self) -> None:
        """
        Read access entities of local storages. Without shown secrets ClickHouse prints
        the type of a password but not its hash, with no [HIDDEN] mark.
        """
        statements = self._source_ctl.get_local_access_statements()
        if not statements:
            return
        storages = self._local_ctl.get_access_storage_types()
        if (
            "local_directory" not in storages
            or not storages <= ALLOWED_ACCESS_STORAGE_TYPES
        ):
            raise ClickhouseBackupError(
                f"Access entities can be restored only into local storage, this host has "
                f"{', '.join(sorted(storages))}. Run with --schema --udf --nc --workload to skip them"
            )
        for statement in statements:
            if HIDDEN_PASSWORD_RE.search(statement):
                raise ClickhouseBackupError(
                    f"Passwords of users on {self._host} are hidden, so users would be "
                    f"created without them: {SECRETS_HINT} on {self._host}"
                )
            self._check("access entities", statement)
        self._access_statements = statements

    def _check_source(self) -> None:
        """
        Fail unless the source is another replica of this shard on a supported version.
        """
        for host, ctl in (
            ("this host", self._local_ctl),
            (self._host, self._source_ctl),
        ):
            if not ctl.ch_version_ge(MIN_CLICKHOUSE_VERSION):
                raise ClickhouseBackupError(
                    f"Restore from a replica needs ClickHouse {MIN_CLICKHOUSE_VERSION} or newer, "
                    f"{host} has {ctl.get_version()}"
                )
        local_version = self._local_ctl.get_version()
        source_version = self._source_ctl.get_version()
        if local_version.split(".")[:2] != source_version.split(".")[:2]:
            raise ClickhouseBackupError(
                f"ClickHouse {local_version} on this host and {source_version} on {self._host} "
                "differ: the schema of another version may not apply here"
            )
        if local_version != source_version:
            logging.warning(
                f"ClickHouse {local_version} on this host and {source_version} on {self._host} "
                "differ in the patch version"
            )
        local_macros = self._local_ctl.get_macros()
        source_macros = self._source_ctl.get_macros()
        local_replica = local_macros.pop("replica", None)
        source_replica = source_macros.pop("replica", None)
        if not local_replica or not source_replica:
            raise ClickhouseBackupError(
                "The replica macro must be set on both hosts to tell the replicas apart"
            )
        if local_replica == source_replica:
            raise ClickhouseBackupError(
                f"{self._host} is this host itself, specify another replica of the shard"
            )
        if local_macros != source_macros:
            raise ClickhouseBackupError(
                f"{self._host} is not a replica of this shard: its macros {source_macros} "
                f"differ from {local_macros}"
            )
        if not self._zookeeper_hosts:
            raise ClickhouseBackupError(
                "ZooKeeper is not configured for ch-backup, but it is needed to remove the "
                "replica of the previous installation of this host"
            )
        if self._replica_name == source_replica:
            raise ClickhouseBackupError(
                f"Replica {self._replica_name} to remove from ZooKeeper is the source "
                f"{self._host}, removing it would break the source"
            )

    def _check(self, name: str, statement: str) -> str:
        """
        Fail if a statement has hidden secrets or a replica name without the macro.
        """
        if "[HIDDEN]" in statement:
            raise ClickhouseBackupError(
                f"Secrets of {name} on {self._host} are hidden, so it would not work here: "
                f"{SECRETS_HINT} on {self._host}"
            )
        for match in REPLICA_ARGUMENT_RE.finditer(statement):
            replica = match.group("table") or match.group("db")
            if "{replica}" not in replica:
                raise ClickhouseBackupError(
                    f"{name} gets replica name {replica} instead of the {{replica}} macro, "
                    "so this host would take over that replica. Check its schema on "
                    f"{self._host} and override_replica_name"
                )
        return statement
