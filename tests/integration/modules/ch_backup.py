"""
Interface to ch-backup command-line tool.
"""

import json
import os
from copy import copy
from typing import Sequence, Set, Union

import yaml

from . import docker, s3, utils
from .typing import ContextT

CH_BACKUP_CLI_PATH = "/usr/local/bin/ch-backup"
CH_BACKUP_CONF_PATH = "/etc/yandex/ch-backup/ch-backup.conf"

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
        return self._metadata.get("meta", {})

    @property
    def metadata(self) -> dict:
        """
        Full backup struct.
        """
        return self._metadata

    @property
    def link_count(self) -> int:
        """
        The number of links (deduplicated parts).
        """
        count = 0
        for db_obj in self._metadata["databases"].values():
            for table_obj in db_obj["tables"].values():
                for part_obj in table_obj["parts"].values():
                    if part_obj["link"]:
                        count += 1
        return count

    @property
    def data_count(self) -> int:
        """
        The number of data parts (not deduplicated).
        """
        count = 0
        for db_obj in self._metadata["databases"].values():
            for table_obj in db_obj["tables"].values():
                for part_obj in table_obj["parts"].values():
                    if not part_obj["link"]:
                        count += 1
        return count

    @property
    def acl_count(self) -> int:
        """
        The number of access control entities.
        """
        return len(self._metadata.get("access_controls", {}).get("acl_ids", []))

    @property
    def udf_count(self) -> int:
        """
        The number of user defined functions.
        """
        return len(self._metadata.get("user_defined_functions", []))

    @property
    def schema_only(self) -> bool:
        """
        Current value for `schema_only` meta flag.
        """
        return bool(self._metadata.get("meta", {}).get("schema_only"))

    @property
    def name(self):
        """
        Backup name.
        """
        return self.meta.get("name")

    @property
    def state(self):
        """
        Backup state.
        """
        return self.meta.get("state")

    @property
    def start_time(self):
        """
        Backup start time
        """
        return self.meta.get("start_time")

    @property
    def end_time(self):
        """
        Backup end time
        """
        return self.meta.get("end_time")

    @property
    def time_format(self):
        """
        Backup time format
        """
        return self.meta.get("time_format")

    @property
    def metadata_path(self) -> str:
        """
        Path to full backup metadata file.
        """
        return os.path.join(self.meta["path"], "backup_struct.json")

    @property
    def light_metadata_path(self) -> str:
        """
        Path to light backup metadata file.
        """
        return os.path.join(self.meta["path"], "backup_light_struct.json")

    def update(self, metadata: dict, merge: bool = True) -> None:
        """
        Update metadata.
        """
        if merge:
            utils.merge(self._metadata, metadata)
        else:
            self._metadata = metadata

    def dump_json(self, *, light: bool = False, indent: int = 4) -> str:
        """
        Dump backup metadata to JSON representation.
        """
        metadata = copy(self._metadata)
        if light:
            metadata["databases"] = {}
            metadata["access_controls"] = {}

        return json.dumps(metadata, indent=indent)

    def get_file_paths(self) -> Sequence[str]:
        """
        Return all storage paths
        """
        backup_path = self.meta["path"]
        cloud_stage_disks = set(self._metadata["cloud_storage"]["disks"])
        file_paths: Set[str] = set()
        for db_name, db_obj in self._metadata["databases"].items():
            for table_name, table_obj in db_obj["tables"].items():
                for part_name, part_obj in table_obj["parts"].items():
                    # Skip S3 parts.
                    if part_obj.get("disk_name") in cloud_stage_disks:
                        continue
                    part_path = os.path.join(
                        part_obj.get("link") or backup_path,
                        "data",
                        db_name,
                        table_name,
                        part_name,
                    )
                    if part_obj.get("tarball", False):
                        file_paths.add(os.path.join(part_path, f"{part_name}.tar"))
                    else:
                        file_paths.update(
                            os.path.join(part_path, f) for f in part_obj["files"]
                        )

        return tuple(file_paths)


class BackupManager:
    """
    Backup manager.
    """

    def __init__(self, context: ContextT, node_name: str, timeout: int = 300) -> None:
        self._container = docker.get_container(context, node_name)
        self._s3_client = s3.S3Client(context)
        self._config_path = CH_BACKUP_CONF_PATH
        protocol = context.ch_backup["protocol"]
        self._cmd_base = f"timeout {timeout} {CH_BACKUP_CLI_PATH} --protocol {protocol} --insecure  --config {self._config_path}"

    # pylint: disable=too-many-arguments
    def backup(
        self,
        name: str = "{uuid}",
        force: bool = None,
        databases: Sequence[str] = None,
        tables: Sequence[str] = None,
        labels: dict = None,
        schema_only: bool = False,
        access: bool = None,
        data: bool = None,
        schema: bool = None,
        udf: bool = None,
        nc: bool = None,
    ) -> str:
        """
        Execute backup command.
        """
        options = [f"--name {name}"]
        if force:
            options.append("--force")
        if databases:
            options.append(f'--databases {",".join(databases or [])}')
        if tables:
            options.append(f'--tables {",".join(tables or [])}')
        for key, value in (labels or {}).items():
            options.append(f"--label {key}={value}")
        if schema_only:
            options.append("--schema-only")
        if access:
            options.append("--access")
        if data:
            options.append("--data")
        if schema:
            options.append("--schema")
        if udf:
            options.append("--udf")
        if nc:
            options.append("--nc")
        return self._exec(f'backup {" ".join(options)}').strip()

    def delete(
        self, backup_id: BackupId, purge_partial: bool = False, force: bool = False
    ) -> str:
        """
        Execute delete command.
        """
        backup_id = self._normalize_id(backup_id)

        options = []
        if purge_partial:
            options.append("--purge-partial")
        if force:
            options.append("--force")

        return self._exec(f'delete {" ".join(options)} {backup_id}')

    def purge(self) -> str:
        """
        Execute purge command.
        """
        return self._exec("purge")

    def version(self) -> str:
        """
        Execute version command.
        """
        return self._exec("version")

    def update_config(self, update: dict) -> None:
        """
        Apply new config to old one
        """
        output = self._container.exec_run(
            f"/bin/cat {self._config_path}", user="root"
        ).output.decode()
        conf = yaml.load(output, yaml.SafeLoader)

        utils.merge(conf, update)
        docker.put_file(
            self._container,
            yaml.dump(conf, default_flow_style=False, encoding="utf-8", indent=4),
            self._config_path,
        )

    def get_backup_ids(self, list_all: bool = True) -> Sequence[str]:
        """
        Get list of existing backup entries / identifiers.
        """
        cmd = "list" if not list_all else "list -a"
        output = self._exec(cmd)
        return list(filter(None, output.split("\n")))

    def get_backup(self, backup_id: BackupId) -> Backup:
        """
        Get backup entry metadata.
        """
        backup_id = self._normalize_id(backup_id)
        output = self._exec(f"show {backup_id}")
        return Backup(json.loads(output))

    # pylint: disable=too-many-arguments
    def restore(
        self,
        backup_id: BackupId,
        schema_only: bool = False,
        override_replica_name: str = None,
        force_non_replicated: bool = False,
        no_clean_zookeeper: bool = False,
        replica_name: str = None,
        cloud_storage_source_bucket: str = None,
        cloud_storage_source_path: str = None,
        access: bool = None,
        data: bool = None,
        schema: bool = None,
        udf: bool = None,
        nc: bool = None,
        keep_going: bool = False,
    ) -> str:
        """
        Restore backup entry.
        """
        backup_id = self._normalize_id(backup_id)
        options = []
        if schema_only:
            options.append("--schema-only")
        if override_replica_name:
            options.append(f"--override-replica-name {override_replica_name}")
        if force_non_replicated:
            options.append("--force-non-replicated")
        if no_clean_zookeeper:
            options.append("--no-clean-zookeeper")
        if replica_name:
            options.append(f"--replica-name {replica_name}")
        if cloud_storage_source_bucket:
            options.append(
                f"--cloud-storage-source-bucket {cloud_storage_source_bucket}"
            )
        if cloud_storage_source_path:
            options.append(f"--cloud-storage-source-path {cloud_storage_source_path}")
        if access:
            options.append("--access")
        if data:
            options.append("--data")
        if schema:
            options.append("--schema")
        if udf:
            options.append("--udf")
        if nc:
            options.append("--nc")
        if keep_going:
            options.append("--keep-going")
        return self._exec(f'restore {" ".join(options)} {backup_id}')

    def restore_access_control(self, backup_id: BackupId) -> str:
        """
        Restore access control metadata from backup.
        """
        backup = self.get_backup(backup_id)
        return self._exec(f"restore-access-control {backup.name}")

    def update_backup_metadata(
        self, backup_id: BackupId, metadata: dict, merge: bool = True
    ) -> None:
        """
        Update backup metadata.
        """
        backup = self.get_backup(backup_id)
        backup.update(metadata, merge)
        self._s3_client.upload_data(
            backup.dump_json(light=False).encode("utf-8"), backup.metadata_path
        )
        self._s3_client.upload_data(
            backup.dump_json(light=True).encode("utf-8"), backup.light_metadata_path
        )

    def delete_backup_metadata_paths(
        self, backup_id: BackupId, paths: Sequence[str]
    ) -> None:
        """
        Delete paths from backup metadata.
        """

        def delete_path(obj: dict, path: Sequence[str]) -> None:
            if len(path) == 1:
                del obj[path[0]]
                return
            delete_path(obj[path[0]], path[1:])

        backup = self.get_backup(backup_id)
        for path in paths:
            delete_path(backup.metadata, path.split("."))
        self._s3_client.upload_data(
            backup.dump_json(light=False).encode("utf-8"), backup.metadata_path
        )
        self._s3_client.upload_data(
            backup.dump_json(light=True).encode("utf-8"), backup.light_metadata_path
        )

    def delete_backup_file(self, backup_id: BackupId, path: str) -> None:
        """
        Delete particular file from backup (useful for fault injection).
        """
        backup = self.get_backup(backup_id)
        self._s3_client.delete_data(os.path.join(backup.meta["path"], path))

    def set_backup_file_data(self, backup_id: BackupId, path: str, data: bytes) -> None:
        """
        Set particular file data in backup (useful for fault injection).
        """
        backup = self.get_backup(backup_id)
        self._s3_client.upload_data(data, os.path.join(backup.meta["path"], path))

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

    def _exec(self, command: str) -> str:
        cmd = f"{self._cmd_base} {command}"
        result = self._container.exec_run(cmd, user="root")
        assert (
            result.exit_code == 0
        ), f'execution failed with code {result.exit_code}, out: "{result.output.decode()}'

        return result.output.decode().strip()

    def _normalize_id(self, backup_id: BackupId) -> str:
        if isinstance(backup_id, int):
            return self.get_backup_ids()[backup_id]
        return backup_id


def get_version() -> str:
    """
    Get ch-backup version.
    """
    with open("ch_backup/version.txt", encoding="utf-8") as f:
        return f.read().strip()
