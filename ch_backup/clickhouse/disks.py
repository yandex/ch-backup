"""
Clickhouse-disks controls temporary cloud storage disks management.
"""

import copy
import os
from concurrent.futures import ThreadPoolExecutor
from subprocess import PIPE, Popen
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import xmltodict

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.models import Disk, Table
from ch_backup.config import Config


class ClickHouseDisksException(RuntimeError):
    """
    ClickHouse-disks call error.
    """

    pass


CH_DISK_CONFIG_PATH = "/tmp/clickhouse-disks-config.xml"


class ClickHouseTemporaryDisks:
    """
    Manages temporary cloud storage disks.
    """

    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        ch_ctl: ClickhouseCTL,
        backup_layout: BackupLayout,
        config: Config,
        backup_meta: BackupMetadata,
        source_bucket: Optional[str],
        source_path: Optional[str],
        source_endpoint: Optional[str],
        ch_config: ClickhouseConfig,
    ):
        self._ch_ctl = ch_ctl
        self._backup_layout = backup_layout
        self._config = config["backup"]
        self._config_dir = config["clickhouse"]["config_dir"]
        self._backup_meta = backup_meta
        self._ch_config = ch_config
        self._source_bucket: str = source_bucket or ""
        self._source_path: str = source_path or ""
        self._source_endpoint: str = source_endpoint or ""
        if self._backup_meta.cloud_storage.enabled and source_bucket is None:
            raise RuntimeError(
                "Backup contains cloud storage data, cloud-storage-source-bucket must be set."
            )

        self._disks: Dict[str, Dict] = {}
        self._created_disks: Dict[str, Disk] = {}
        self._ch_availible_disks: Dict[str, Disk] = {}

    def __enter__(self):
        self._disks = self._ch_config.config["storage_configuration"]["disks"]
        for disk_name in self._backup_meta.cloud_storage.disks:
            self._create_temporary_disk(
                self._backup_meta,
                disk_name,
                self._source_bucket,
                self._source_path,
                self._source_endpoint,
            )
        self._backup_layout.wait()
        self._ch_availible_disks = self._ch_ctl.get_disks()
        self._render_disks_config(
            CH_DISK_CONFIG_PATH,
            {
                name: conf
                for name, conf in self._disks.items()
                if not conf or conf.get("type") != "cache"
            },
        )
        return self

    def __exit__(self, exc_type, *args, **kwargs):
        if exc_type is not None:
            logging.warning(
                f'Omitting tmp cloud storage disk cleanup due to exception: "{exc_type}"'
            )
            return False

        for disk in self._created_disks.values():
            logging.debug(f"Removing tmp disk {disk.name}")
            try:
                os.remove(_get_config_path(self._config_dir, disk.name))
                self._disks.pop(disk.name)
                return True
            except FileNotFoundError:
                pass

    def _render_disks_config(self, path, disks):
        with open(path, "w", encoding="utf-8") as f:
            xmltodict.unparse(
                {
                    "yandex": {
                        "storage_configuration": {"disks": disks},
                    }
                },
                f,
                pretty=True,
            )

    def _create_temporary_disk(
        self,
        backup_meta: BackupMetadata,
        disk_name: str,
        source_bucket: str,
        source_path: str,
        source_endpoint: str,
    ) -> None:
        tmp_disk_name = _get_tmp_disk_name(disk_name)
        logging.debug(f"Creating tmp disk {tmp_disk_name}")
        disk_config = copy.copy(
            self._ch_config.config["storage_configuration"]["disks"][disk_name]
        )

        endpoint = urlparse(disk_config["endpoint"])
        endpoint_netloc = source_endpoint or endpoint.netloc
        disk_config["endpoint"] = os.path.join(
            f"{endpoint.scheme}://{endpoint_netloc}", source_bucket, source_path, ""
        )
        self._render_disks_config(
            _get_config_path(self._config_dir, tmp_disk_name),
            {tmp_disk_name: disk_config},
        )

        self._ch_ctl.reload_config()
        source_disk = self._ch_ctl.get_disk(tmp_disk_name)
        logging.debug(f'Restoring Cloud Storage "shadow" data  of disk "{disk_name}"')
        self._backup_layout.download_cloud_storage_metadata(
            backup_meta, source_disk, disk_name
        )

        self._created_disks[tmp_disk_name] = source_disk
        self._disks[tmp_disk_name] = disk_config

    def copy_parts(
        self,
        backup_meta: BackupMetadata,
        parts_to_copy: List[Tuple[Table, PartMetadata]],
        max_proccesses_count: int,
        keep_going: bool,
    ) -> None:
        """
        Copy parts from temporary cloud storage disk to actual.

        If clickhouse greater or equal than 24.1 then we are able to use s3-server-side copy.
        Spawns no more than max_processes_count of clickhouse-disks subproceses to copy part from tmp disk.
        """

        with ThreadPoolExecutor(max_workers=max_proccesses_count) as executor:
            # Can't use map function here. The map method returns a generator
            # and it is not possible to resume a generator after an exception occurs.
            # https://peps.python.org/pep-0255/#specification-generators-and-exception-propagation
            futures_to_part = {
                executor.submit(
                    self._run_copy_command, backup_meta, part[0], part[1]
                ): part[1].name
                for part in parts_to_copy
            }
            for future in futures_to_part:
                try:
                    future.result()
                except Exception:
                    if keep_going:
                        part_name = futures_to_part[future]
                        logging.exception(
                            f"Restore of part {part_name} failed, skipping due to --keep-going flag"
                        )
                    else:
                        raise

    def _run_copy_command(
        self, backup_meta: BackupMetadata, table: Table, part: PartMetadata
    ) -> None:
        """
        Copy data from temporary cloud storage disk to actual.
        """
        routine_tag = f"{table.database}.{table.name}::{part.name}"
        target_disk = self._ch_availible_disks[part.disk_name]
        source_disk = self._ch_availible_disks[_get_tmp_disk_name(part.disk_name)]
        for path, disk in table.paths_with_disks:
            logging.info(f"Target {target_disk.name} current {disk.name}")
            if disk.name == target_disk.name:
                table_path = os.path.relpath(path, target_disk.path)
                target_path = os.path.join(table_path, "detached")
                if self._ch_ctl.ch_version_ge("23.7"):
                    target_path = os.path.join(target_path, part.name, "")
                source_path = os.path.join(
                    "shadow",
                    backup_meta.get_sanitized_name(),
                    table_path,
                    part.name,
                    "",
                )
                self._copy_dir(
                    source_disk.name,
                    source_path,
                    target_disk.name,
                    target_path,
                    routine_tag,
                )
                return

        raise RuntimeError(
            f'Disk "{target_disk.name}" path not found for table `{table.database}`.`{table.name}`'
        )

    def _copy_dir(
        self,
        from_disk: str,
        from_path: str,
        to_disk: str,
        to_path: str,
        routine_tag: str,
    ) -> None:
        if self._ch_ctl.ch_version_ge("23.9"):
            command_args = [
                "--disk-from",
                from_disk,
                "--disk-to",
                to_disk,
                from_path,
                to_path,
            ]
        else:
            command_args = [
                "--diskFrom",
                from_disk,
                "--diskTo",
                to_disk,
                from_path,
                to_path,
            ]

        result = _exec(
            routine_tag,
            "copy",
            common_args=["-C", CH_DISK_CONFIG_PATH],
            command_args=command_args,
        )
        logging.info(f"clickhouse-disks copy result for {routine_tag}: {result}")


def _get_config_path(config_dir: str, disk_name: str) -> str:
    return os.path.join(config_dir, f"cloud_storage_tmp_disk_{disk_name}.xml")


def _get_tmp_disk_name(disk_name: str) -> str:
    return f"{disk_name}_source"


def _exec(
    routine_tag: str, command: str, common_args: List[str], command_args: List[str]
) -> Any:

    ch_disks_logger = logging.getLogger("clickhouse-disks").bind(tag=routine_tag)
    command_args = [
        "/usr/bin/clickhouse-disks",
        *common_args,
        command,
        *command_args,
    ]

    logging.debug(f'Executing "{" ".join(command_args)}"')

    with Popen(command_args, stdout=PIPE, stderr=PIPE, shell=False) as proc:
        while proc.poll() is None:
            for line in proc.stderr.readlines():  # type: ignore
                ch_disks_logger.info(line.decode("utf-8").strip())
        if proc.returncode != 0:
            raise ClickHouseDisksException(
                f"clickhouse-disks call failed with exitcode: {proc.returncode}"
            )

        return list(map(lambda b: b.decode("utf-8"), proc.stdout.readlines()))  # type: ignore
