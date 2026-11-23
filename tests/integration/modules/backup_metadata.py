"""Access backup objects from Behave without running Python in containers."""

import copy
import json
from urllib.parse import quote

import yaml

from ch_backup.backup.layout import BackupLayout
from ch_backup.clickhouse.object_storage_metadata import parse_object_storage_metadata
from ch_backup.config import Config
from ch_backup.storage import StorageLoader
from ch_backup.storage.engine import get_storage_engine

from . import docker
from .ch_backup_cli import CH_BACKUP_CONF_PATH
from .typing import ContextT


class BackupStorage:
    """Use a node's backup configuration through the exposed test S3 endpoint."""

    def __init__(self, context: ContextT, node: str, bucket: str | None = None) -> None:
        self.container = docker.get_container(context, node)
        result = self.container.exec_run(["cat", CH_BACKUP_CONF_PATH], user="root")
        assert result.exit_code == 0, "Cannot read ch-backup configuration"
        self.node_config = yaml.safe_load(result.output)
        self.config = Config("/dev/null")
        self.config.merge(self.node_config)
        host, port = docker.get_exposed_port(
            docker.get_container(context, context.conf["s3"]["container"]),
            context.conf["s3"]["port"],
        )
        self.config["storage"]["credentials"]["endpoint_url"] = f"http://{host}:{port}"
        self.config["storage"]["proxy_resolver"]["uri"] = None
        self.config["multiprocessing"]["workers"] = 0
        if bucket is not None:
            self.config["storage"]["credentials"]["bucket"] = bucket
        self.layout = BackupLayout(self.config)
        self.loader = StorageLoader(self.config)
        self.engine = get_storage_engine(self.config["storage"])

    def create_keyless_config(
        self,
        path: str,
        backup_name: str | None = None,
        disk: str | None = None,
        source: dict | None = None,
    ) -> None:
        """Create a copy worker config with neither a key nor local ClickHouse config."""
        worker = copy.deepcopy(self.node_config)
        worker["encryption"].pop("key", None)
        worker["clickhouse"][
            "preprocessed_config_path"
        ] = "/tmp/no-clickhouse-config.xml"
        if source is not None:
            assert backup_name and disk, "A cloud source needs a backup and a disk"
            backup = self.layout.get_backup(backup_name)
            assert backup is not None
            paths = self.layout.get_cloud_storage_metadata_remote_paths(
                backup.name, disk, backup.cloud_storage.compressed, "all"
            )
            metadata = [
                parse_object_storage_metadata(data)
                for remote_path in paths
                for _, data in self.layout.get_cloud_storage_metadata_files(
                    backup, remote_path
                )
            ]
            assert metadata, "Expected S3 disk metadata in the backup"
            source = source.copy()
            # V5 stores complete keys; older formats need the original disk prefix.
            if all(item.full_object_keys for item in metadata):
                source.pop("path", None)
            worker.setdefault("copy", {}).setdefault("cloud_storage", {})[
                "source"
            ] = source
        docker.put_file(self.container, yaml.safe_dump(worker).encode(), path)

    def rewrite_legacy_metadata(self, backup_name: str) -> None:
        """Simulate old metadata without encryption flags and with encrypted full JSON."""
        for filename in ("backup_struct.json", "backup_light_struct.json"):
            path = f"{self.layout.get_backup_path(backup_name)}/{filename}"
            data = json.loads(self.loader.download_data(path))
            data["meta"].pop("metadata_encrypted")
            data["meta"].pop("encrypted")
            data["cloud_storage"].pop("encryption")
            for database in data["databases"].values():
                for table in database["tables"].values():
                    for part in table["parts"].values():
                        part.pop("encrypted")
            self.loader.upload_data(
                json.dumps(data), path, encryption=filename == "backup_struct.json"
            )

    def rewrite_per_file_parts(self, backup_name: str) -> None:
        """Store real part files with the unaligned sizes used by old backups."""
        backup = self.layout.get_backup(backup_name)
        assert backup is not None
        for table in backup.get_tables():
            for part in table.get_parts():
                assert part.tarball and not part.link
                path = "/".join(
                    [
                        self.layout.get_backup_path(backup_name),
                        "data",
                        quote(part.database, safe=""),
                        quote(part.table, safe=""),
                        part.name,
                    ]
                )
                archive = f"{path}/{part.name}.tar"
                files = dict(
                    self.loader.download_data_tarball(
                        archive, encryption=part.encrypted
                    )
                )
                for name, data in files.items():
                    self.loader.upload_data(
                        data, f"{path}/{name}", encryption=part.encrypted
                    )
                size = sum(map(len, files.values()))
                backup.size += size - part.size
                table.raw_metadata["parts"][part.name].update(
                    tarball=False,
                    bytes=size,
                    files=list(files),
                )
                self.engine.delete_file(archive)
        self.layout.upload_backup_metadata(backup)

    def raw_objects(self, backup_name: str) -> dict[str, bytes]:
        """Read the exact stored bytes, bypassing decryption and metadata parsing."""
        prefix = self.layout.get_backup_path(backup_name).rstrip("/") + "/"
        return {
            path: self.engine.download_data(path)
            for path in self.engine.list_dir(prefix, recursive=True, absolute=True)
        }
