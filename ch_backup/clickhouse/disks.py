"""
Clickhouse-disks controls temporary cloud storage disks management.
"""

import copy
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from subprocess import PIPE, Popen
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type
from urllib.parse import urlparse

import xmltodict

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.models import Disk, Table
from ch_backup.config import Config
from ch_backup.util import is_equal_s3_endpoints


class ClickHouseDisksException(RuntimeError):
    """
    ClickHouse-disks call error.
    """

    pass


CH_DISK_CONFIG_PATH = "/tmp/clickhouse-disks-config.xml"
CH_DISK_HISTORY_FILE_PATH = "/tmp/.disks-file-history"
CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS = 1 * 60 * 60 * 1000


class ClickHouseTemporaryDisks:
    """
    Manages temporary cloud storage disks.
    """

    # pylint: disable=too-many-instance-attributes,too-many-positional-arguments
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
        use_local_copy: bool = False,
    ):
        self._ch_ctl = ch_ctl
        self._backup_layout = backup_layout
        self._config = config["backup"]
        self._config_dir = config["clickhouse"]["config_dir"]
        self._backup_meta = backup_meta
        self._ch_config = ch_config
        self._use_local_copy = use_local_copy
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

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        if exc_type is not None:
            logging.warning(
                f'Omitting tmp cloud storage disk cleanup due to exception: "{exc_type.__name__}: {value}"'
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
        return True

    def _render_disks_config(self, path, disks):
        with open(path, "w", encoding="utf-8") as f:
            xmltodict.unparse(
                {
                    "clickhouse": {
                        "storage_configuration": {"disks": disks},
                        "history-file": CH_DISK_HISTORY_FILE_PATH,
                    },
                },
                f,
                pretty=True,
            )

    # pylint: disable=too-many-positional-arguments
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

        tmp_disk_enpoint = os.path.join(
            f"{endpoint.scheme}://{endpoint_netloc}", source_bucket, source_path, ""
        )
        orig_disk_enpoint = self._ch_config.config["storage_configuration"]["disks"][
            disk_name
        ]["endpoint"]

        if self._use_local_copy and not is_equal_s3_endpoints(
            tmp_disk_enpoint, orig_disk_enpoint
        ):
            raise RuntimeError(
                f"Endpoint of tmp object storage disk is not equal to original (original {orig_disk_enpoint}  tmp: {tmp_disk_enpoint})."
                "It is required for inplace restore mode."
            )

        disk_config["endpoint"] = tmp_disk_enpoint

        disks_config = {tmp_disk_name: disk_config}

        request_timeout_ms = int(disk_config.get("request_timeout_ms", 0))
        if request_timeout_ms < CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS:
            disks_config[tmp_disk_name]["request_timeout_ms"] = str(
                CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS
            )
            disks_config[disk_name] = {
                "request_timeout_ms": {
                    "@replace": "replace",
                    "#text": str(CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS),
                }
            }
            if self._disks:
                self._disks[disk_name]["request_timeout_ms"] = str(
                    CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS
                )

        disks_config[tmp_disk_name]["skip_access_check"] = str(True).lower()

        self._render_disks_config(
            _get_config_path(self._config_dir, tmp_disk_name),
            disks_config,
        )

        self._ch_ctl.reload_config()
        source_disk = self._ch_ctl.get_disk(tmp_disk_name)
        logging.debug(f'Restoring Cloud Storage "shadow" data of disk "{disk_name}"')
        self._backup_layout.download_cloud_storage_metadata(
            backup_meta, source_disk, disk_name
        )

        self._created_disks[tmp_disk_name] = source_disk
        self._disks[tmp_disk_name] = disks_config[tmp_disk_name]

    def copy_parts(
        self,
        backup_meta: BackupMetadata,
        parts_to_copy: List[Tuple[Table, PartMetadata]],
        max_proccesses_count: int,
        keep_going: bool,
        part_callback: Optional[Callable],
    ) -> None:
        """
        Copy parts from temporary cloud storage disk to actual.

        If clickhouse greater or equal than 24.1 then we are able to use s3-server-side copy.
        Spawns no more than max_processes_count of clickhouse-disks subproceses to copy part from tmp disk.
        """

        if max_proccesses_count > 1 and not self._ch_ctl.ch_version_ge("23.3"):
            logging.warning(
                "It is unsafe to use cloud_storage_restore_workers > 1 with clickhouse version < 23.3"
                f"(cloud_storage_restore_workers: {max_proccesses_count}, ch_version: {self._ch_ctl.get_version()}"
            )
        with ThreadPoolExecutor(max_workers=max_proccesses_count) as executor:
            # Can't use map function here. The map method returns a generator
            # and it is not possible to resume a generator after an exception occurs.
            # https://peps.python.org/pep-0255/#specification-generators-and-exception-propagation
            futures_to_part = {}
            for part in parts_to_copy:
                future = executor.submit(
                    self._run_copy_command,
                    backup_meta,
                    part[0],
                    part[1],
                )
                if part_callback:
                    future.add_done_callback(partial(part_callback, part[1]))
                futures_to_part[future] = part[1].name

            for future in as_completed(futures_to_part):
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

    # pylint: disable=too-many-positional-arguments
    def _copy_dir(
        self,
        from_disk: str,
        from_path: str,
        to_disk: str,
        to_path: str,
        routine_tag: str,
    ) -> None:
        if self._use_local_copy:
            self._os_copy(from_disk, from_path, to_disk, to_path, routine_tag)
        else:
            self._ch_disks_copy(from_disk, from_path, to_disk, to_path, routine_tag)

    # pylint: disable=too-many-positional-arguments
    def _os_copy(
        self,
        from_disk: str,
        from_path: str,
        to_disk: str,
        to_path: str,
        routine_tag: str,
    ) -> None:
        from_full_path = os.path.join(self._ch_ctl.get_disk(from_disk).path, from_path)
        to_full_path = os.path.join(self._ch_ctl.get_disk(to_disk).path, to_path)

        result = _exec(
            routine_tag,
            exe="/bin/cp",
            common_args=["-rf", from_full_path, to_full_path],
        )
        logging.info(f"os copy result for {routine_tag}: {result}")

    # pylint: disable=too-many-positional-arguments
    def _ch_disks_copy(
        self,
        from_disk: str,
        from_path: str,
        to_disk: str,
        to_path: str,
        routine_tag: str,
    ) -> None:
        command = "copy"
        common_args = ["--config", CH_DISK_CONFIG_PATH]
        if self._ch_ctl.ch_version_ge("24.7"):
            command_args = [
                "--recursive",
                "--disk-from",
                from_disk,
                "--disk-to",
                to_disk,
                from_path,
                to_path,
                "'",
            ]
            common_args.append("--query")
            # Changes in disks interface require passing command with args in quotes
            command = "'" + command
        elif self._ch_ctl.ch_version_ge("23.9"):
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
            exe="/usr/bin/clickhouse-disks",
            common_args=common_args,
            command=command,
            command_args=command_args,
        )
        logging.info(f"clickhouse-disks copy result for {routine_tag}: {result}")


def _get_config_path(config_dir: str, disk_name: str) -> str:
    return os.path.join(config_dir, f"cloud_storage_tmp_disk_{disk_name}.xml")


def _get_tmp_disk_name(disk_name: str) -> str:
    return f"{disk_name}_source"


def _exec(
    routine_tag: str,
    exe: str,
    common_args: List[str],
    command: Optional[str] = None,
    command_args: Optional[List[str]] = None,
) -> Any:

    proc_logger = logging.getLogger("clickhouse-disks").bind(tag=routine_tag)
    args = [
        exe,
        *common_args,
    ]
    if command:
        command_with_args = [command, *command_args] if command_args else [command]
        args += command_with_args  # type: ignore

    args = " ".join(args)  # type: ignore
    logging.debug(f'Executing "{args}"')

    with Popen(args, stdout=PIPE, stderr=PIPE, shell=True) as proc:  # nosec
        while proc.poll() is None:
            for line in proc.stderr.readlines():  # type: ignore
                proc_logger.info(line.decode("utf-8").strip())
        if proc.returncode != 0:
            raise ClickHouseDisksException(
                f"{exe} call failed with exitcode: {proc.returncode}"
            )

        return list(map(lambda b: b.decode("utf-8"), proc.stdout.readlines()))  # type: ignore
