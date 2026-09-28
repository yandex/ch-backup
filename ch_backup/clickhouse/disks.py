"""
Clickhouse-disks controls temporary cloud storage disks management.
"""

import copy
import os
import shutil
import threading
from collections import defaultdict
from contextlib import contextmanager, suppress
from functools import partial
from subprocess import PIPE, Popen
from types import TracebackType
from typing import IO, Any, Callable, Iterator, Literal, Sequence
from urllib.parse import urlparse

import xmltodict

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout, table_shadow_relpath
from ch_backup.backup.metadata import (
    BackupMetadata,
    PartMetadata,
    sanitize_backup_name,
)
from ch_backup.backup.metadata.table_metadata import TableMetadata
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.models import Disk, FrozenPart, Table
from ch_backup.config import Config
from ch_backup.storage.async_pipeline.base_pipeline.exec_pool import ThreadExecPool
from ch_backup.util import (
    is_equal_s3_endpoints,
    s3_uri_from_path_style_to_virtual_hosted,
)


class ClickHouseDisksException(RuntimeError):
    """
    ClickHouse-disks call error.
    """

    pass


CH_DISK_CONFIG_PATH = "/tmp/clickhouse-disks-config.xml"
CH_DISK_HISTORY_FILE_PATH = "/tmp/.disks-file-history"
CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS = 1 * 60 * 60 * 1000
BACKUP_DISK_CONFIG_KEYS = (
    "type",
    "object_storage_type",
    "metadata_type",
    "request_timeout_ms",
)
DEDUPLICATED_DIR_SUFFIX = "_deduplicated"


class ClickHouseDiskManager:
    """
    Base for managers of temporary cloud storage disks.
    """

    # pylint: disable=too-many-positional-arguments,too-many-arguments
    def __init__(
        self,
        ch_ctl: ClickhouseCTL,
        backup_layout: BackupLayout,
        config: Config,
        backup_meta: BackupMetadata,
        ch_config: ClickhouseConfig,
    ) -> None:
        self._ch_ctl = ch_ctl
        self._backup_layout = backup_layout
        self._config_dir = config["clickhouse"]["config_dir"]
        self._storage_config = config["storage"]
        self._backup_meta = backup_meta
        self._ch_config = ch_config

        self._disks: dict[str, dict] = {}
        self._created_disks: dict[str, Disk] = {}
        self._disks_lock = threading.Lock()

    def _read_configured_disks(self) -> None:
        """
        Read currently configured disks from the ClickHouse configuration.
        """
        self._disks = self._ch_config.config.get("storage_configuration", {}).get(
            "disks", {}
        )

    def _backup_disk_config(self, backup_name: str, disk_name: str) -> dict:
        """
        Build a disk configuration pointing to data of a disk in the backup.

        Only settings known to be safe for the backup storage are taken from
        the original disk. Others, like a proxy or encryption, belong to the
        storage of the original disk, and a shared local metadata path would
        make the cleanup of the temporary disk remove data of the original one.

        Access check is skipped: it writes to the backup bucket, which is
        already known to be writable, and ClickHouse retries a failed check
        far beyond the time ch-backup waits for SYSTEM RELOAD CONFIG.
        """
        disk_config = {
            key: value
            for key, value in self._disks[disk_name].items()
            if key in BACKUP_DISK_CONFIG_KEYS
        }
        _set_backup_storage(
            disk_config,
            self._storage_config,
            self._backup_layout.get_cloud_storage_data_path(backup_name, disk_name),
        )
        disk_config["skip_access_check"] = str(True).lower()
        return disk_config

    def _register_disk(
        self, disk_name: str, tmp_disk_name: str, disk_config: dict
    ) -> Disk:
        """
        Write configuration of a temporary disk and make ClickHouse pick it up.
        """
        disks_config = {tmp_disk_name: disk_config}
        if _raise_request_timeout(disk_config):
            disks_config[disk_name] = _request_timeout_override()
            if self._disks:
                self._disks[disk_name]["request_timeout_ms"] = str(
                    CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS
                )

        _render_disks_config(
            _get_config_path(self._config_dir, tmp_disk_name),
            disks_config,
        )
        self._ch_ctl.reload_config()

        disk = self._ch_ctl.get_disk(tmp_disk_name)
        self._created_disks[tmp_disk_name] = disk
        self._disks[tmp_disk_name] = disk_config
        return disk

    def _cleanup(
        self, exc_type: type[BaseException] | None, value: BaseException | None
    ) -> bool:
        """
        Remove configuration files and local data of the created disks.

        Configuration files hold credentials, so they go even after a failure.
        Local data is kept for investigation, and ClickHouse is not asked to
        reload its configuration: it may well be the cause of the failure, and
        raising here would hide it.
        """
        if exc_type is not None:
            logging.warning(
                f'Keeping local data of tmp cloud storage disks due to exception: "{exc_type.__name__}: {value}"'
            )

        for tmp_disk_name in self._created_disks:
            logging.debug(f"Removing tmp disk {tmp_disk_name}")
            _remove_file(_get_config_path(self._config_dir, tmp_disk_name))
            self._disks.pop(tmp_disk_name, None)
        _remove_file(CH_DISK_CONFIG_PATH)
        if self._created_disks and exc_type is None:
            self._ch_ctl.reload_config()
            for disk in self._created_disks.values():
                self._on_disk_removed(disk)
        self._created_disks.clear()
        return exc_type is None

    def _on_disk_removed(self, disk: Disk) -> None:
        """
        Clean up what is left on the host by a removed disk.
        """


class ClickHouseTemporaryDisks(ClickHouseDiskManager):
    """
    Manages temporary cloud storage disks.
    """

    # pylint: disable=too-many-positional-arguments,too-many-arguments
    def __init__(
        self,
        ch_ctl: ClickhouseCTL,
        backup_layout: BackupLayout,
        config: Config,
        backup_meta: BackupMetadata,
        source_bucket: str | None,
        source_path: str | None,
        source_endpoint: str | None,
        ch_config: ClickhouseConfig,
        desired_tables: Sequence[TableMetadata] | Literal["all"] = "all",
        use_local_copy: bool = False,
    ):
        super().__init__(ch_ctl, backup_layout, config, backup_meta, ch_config)
        self._use_local_copy = use_local_copy
        self._source_bucket: str = source_bucket or ""
        self._source_path: str = source_path or ""
        self._source_endpoint: str = source_endpoint or ""
        if backup_meta.cloud_storage.requires_source_bucket and source_bucket is None:
            raise RuntimeError(
                "Backup contains cloud storage data, cloud-storage-source-bucket must be set."
            )
        self._desired_tables: Sequence[TableMetadata] | Literal["all"] = desired_tables

        self._ch_availible_disks: dict[str, Disk] = {}

    def __enter__(self):
        self._read_configured_disks()
        for disk_name in self._backup_meta.cloud_storage.disks:
            self._create_temporary_disk(
                self._backup_meta,
                disk_name,
                self._desired_tables,
            )
        for backup_meta, disk_name, tables in self._linked_backups_data():
            self._create_temporary_disk(
                backup_meta, disk_name, tables, link=backup_meta.name
            )
        self._backup_layout.wait()
        self._ch_availible_disks = self._ch_ctl.get_disks()
        _render_ch_disks_config(self._disks)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        return self._cleanup(exc_type, value)

    def _linked_backups_data(
        self,
    ) -> Iterator[tuple[BackupMetadata, str, list[TableMetadata]]]:
        """
        Yield backups holding data of deduplicated parts on cloud storage disks.

        Such data stays in the backup it was copied into and is read through a
        temporary disk of that backup.
        """
        if not self._backup_meta.cloud_storage.disks:
            return

        links: dict[str, dict[str, dict[tuple[str, str], TableMetadata]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        for table in self._tables_to_restore():
            for part in table.get_parts():
                if not part.link:
                    continue
                if part.disk_name not in self._backup_meta.cloud_storage.disks:
                    continue

                links[part.link][part.disk_name][(table.database, table.name)] = table

        for backup_name, disks in links.items():
            backup_meta = self._backup_layout.get_backup(
                backup_name, use_light_meta=True
            )
            if backup_meta is None or not backup_meta.cloud_storage.data_copied:
                raise ClickHouseDisksException(
                    f'Backup "{backup_name}" holding data of deduplicated parts'
                    " is missing or holds no copied data"
                )
            for disk_name, tables in disks.items():
                yield backup_meta, disk_name, list(tables.values())

    def _tables_to_restore(self) -> Sequence[TableMetadata]:
        """
        Return tables of the backup whose data is going to be restored.
        """
        if self._desired_tables == "all":
            return self._backup_meta.get_tables()

        return self._desired_tables

    def _create_temporary_disk(
        self,
        backup_meta: BackupMetadata,
        disk_name: str,
        desired_tables: Sequence[TableMetadata] | Literal["all"] = "all",
        link: str | None = None,
    ) -> None:
        """
        Create a temporary disk with data of a disk in a given backup.

        Inplace restore copies metadata files alone, and they name objects
        relative to the disk endpoint. Objects copied into the backup are not
        reachable from the original disk, so such backups are refused.
        """
        if self._use_local_copy and backup_meta.cloud_storage.data_copied:
            raise ClickHouseDisksException(
                f'Backup "{backup_meta.name}" holds a copy of cloud storage data,'
                " it cannot be restored with use_inplace_cloud_restore."
            )

        tmp_disk_name = _get_tmp_disk_name(disk_name, link)
        logging.debug(f"Creating tmp disk {tmp_disk_name}")
        if disk_name not in self._disks:
            raise ClickHouseDisksException(
                f'Disk "{disk_name}" is present in backup cloud storage metadata'
                f" but is missing from ClickHouse storage_configuration."
                f" Add the disk to the ClickHouse configuration and retry."
            )

        orig_disk_endpoint = self._disks[disk_name]["endpoint"]
        disk_config = self._source_disk_config(backup_meta, disk_name)
        tmp_disk_endpoint = disk_config["endpoint"]

        if self._use_local_copy and not is_equal_s3_endpoints(
            tmp_disk_endpoint, orig_disk_endpoint
        ):
            raise RuntimeError(
                f"Endpoint of tmp object storage disk is not equal to original (original {orig_disk_endpoint}  tmp: {tmp_disk_endpoint})."
                "It is required for inplace restore mode."
            )

        disk_config["skip_access_check"] = str(True).lower()
        source_disk = self._register_disk(disk_name, tmp_disk_name, disk_config)

        logging.debug(f'Restoring Cloud Storage "shadow" data of disk "{disk_name}"')
        self._backup_layout.download_cloud_storage_metadata(
            backup_meta,
            source_disk,
            disk_name,
            desired_tables,
        )

    def _source_disk_config(self, backup_meta: BackupMetadata, disk_name: str) -> dict:
        """
        Build a disk configuration pointing to the location of the data.
        """
        if backup_meta.cloud_storage.data_copied:
            return self._backup_disk_config(backup_meta.name, disk_name)

        disk_config = copy.copy(self._disks[disk_name])
        endpoint = urlparse(disk_config["endpoint"])
        disk_config["endpoint"] = os.path.join(
            f"{endpoint.scheme}://{self._source_endpoint or endpoint.netloc}",
            self._source_bucket,
            self._source_path,
            "",
        )
        return disk_config

    def copy_parts(
        self,
        backup_meta: BackupMetadata,
        parts_to_copy: list[tuple[Table, PartMetadata]],
        max_proccesses_count: int,
        keep_going: bool,
        part_callback: Callable,
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
        with ThreadExecPool(max_proccesses_count) as executor:
            for part in parts_to_copy:
                executor.submit(
                    f"Restore of part {part[1].name}",
                    self._run_copy_command,
                    backup_meta,
                    part[0],
                    part[1],
                    callback=partial(part_callback, part[1]),
                )
            executor.wait_all(keep_going)

    def _run_copy_command(
        self, backup_meta: BackupMetadata, table: Table, part: PartMetadata
    ) -> None:
        """
        Copy data from temporary cloud storage disk to actual.
        """
        source_part_name = part.deduplicated_part_name
        source_backup_name = part.link or backup_meta.name

        routine_tag = f"{table.database}.{table.name}::{source_part_name}"
        target_disk = self._ch_availible_disks[part.disk_name]
        source_disk = self._ch_availible_disks[
            _get_tmp_disk_name(part.disk_name, part.link)
        ]
        for path, disk in table.paths_with_disks:
            if disk.name == target_disk.name:
                table_path = os.path.relpath(path, target_disk.path)
                target_path = os.path.join(table_path, "detached")
                if self._ch_ctl.ch_version_ge("23.7"):
                    target_path = os.path.join(target_path, part.name, "")
                source_path = os.path.join(
                    "shadow",
                    sanitize_backup_name(source_backup_name),
                    table_path,
                    source_part_name,
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
            _ch_disks_copy(
                self._ch_ctl, from_disk, from_path, to_disk, to_path, routine_tag
            )

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


class ClickHouseBackupDisks(ClickHouseDiskManager):
    """
    Manages temporary cloud storage disks pointing to the backup bucket.

    Data is copied from several threads, while neither the ClickHouse client
    nor the configuration files are meant for concurrent use. So disks are
    created upfront, and a disk missed there is created under a lock.
    """

    def __enter__(self) -> "ClickHouseBackupDisks":
        self._read_configured_disks()
        _render_ch_disks_config(self._disks)
        try:
            for disk in self._ch_ctl.get_disks().values():
                if (
                    disk.type == "s3"
                    and not disk.cache_path
                    and disk.name in self._disks
                ):
                    self.create_disk(disk.name)
        except BaseException as e:
            self._cleanup(type(e), e)
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._cleanup(exc_type, value)

    def _on_disk_removed(self, disk: Disk) -> None:
        """
        Remove metadata of the copied objects left on the local disk.

        Failure is not ignored: unfreezing the backup afterwards would remove
        the copied objects through the metadata left behind.
        """
        with suppress(FileNotFoundError):
            shutil.rmtree(os.path.join(disk.path, "shadow"))

    def create_disk(self, disk_name: str) -> Disk:
        """
        Create a temporary disk that writes to the backup bucket.

        Returns the already created disk if called for the same disk twice.
        """
        tmp_disk_name = _get_backup_disk_name(disk_name)
        with self._disks_lock:
            if tmp_disk_name in self._created_disks:
                return self._created_disks[tmp_disk_name]

            if disk_name not in self._disks:
                raise ClickHouseDisksException(
                    f'Disk "{disk_name}" is missing from ClickHouse storage_configuration.'
                )

            logging.debug(f"Creating tmp disk {tmp_disk_name}")
            disk = self._register_disk(
                disk_name,
                tmp_disk_name,
                self._backup_disk_config(self._backup_meta.name, disk_name),
            )
            _render_ch_disks_config(self._disks)
            return disk

    def exclude_frozen_parts(
        self, disks: dict[str, Disk], parts: Sequence[FrozenPart]
    ) -> None:
        """
        Move frozen data of a batch of parts aside from the data of their table.

        Such parts are left out of the copy, and their frozen data is released by
        SYSTEM UNFREEZE at the end of the backup: ClickHouse decrements the
        reference count kept in the metadata of the objects and, on a disk with
        zero-copy replication, asks ZooKeeper whether a replica still needs them.
        """
        for part in parts:
            part_path = os.path.relpath(part.path, disks[part.disk_name].path)
            if not part_path.startswith("shadow/"):
                raise ClickHouseDisksException(
                    f'Path "{part.path}" of part {part.name} holds no frozen data'
                )

            excluded_dir = os.path.dirname(part.path) + DEDUPLICATED_DIR_SUFFIX
            if not os.path.isdir(excluded_dir):
                os.mkdir(excluded_dir)
                self._ch_ctl.chown_dir(excluded_dir)
            os.rename(part.path, os.path.join(excluded_dir, part.name))

    def copy_table_data(self, disk_name: str, table: Table) -> Disk:
        """
        Copy frozen table data from a given disk into the backup bucket.

        clickhouse-disks creates the last directory of the destination itself,
        fails if the rest is missing and nests the copy one level deeper if
        that directory already exists.
        """
        assert table.path_on_disk, f"Table {table} doesn't store data on disk"

        backup_disk = self.create_disk(disk_name)
        shadow_path = os.path.join(
            table_shadow_relpath(
                self._backup_meta.get_sanitized_name(), table.path_on_disk
            ),
            "",
        )
        target_path = os.path.join(backup_disk.path, shadow_path)
        os.makedirs(os.path.dirname(target_path.rstrip("/")), exist_ok=True)
        _ch_disks_copy(
            self._ch_ctl,
            disk_name,
            shadow_path,
            backup_disk.name,
            shadow_path,
            f"Backup of {table.database}.{table.name} on disk {disk_name}",
        )
        return backup_disk


def _set_backup_storage(
    disk_config: dict, storage_config: dict, key_prefix: str
) -> None:
    """
    Point a disk configuration to a location in the backup storage.
    """
    credentials = storage_config["credentials"]
    disk_config["endpoint"] = _backup_storage_endpoint(storage_config, key_prefix)
    disk_config["access_key_id"] = credentials["access_key_id"]
    disk_config["secret_access_key"] = credentials["secret_access_key"]

    proxy = _backup_storage_proxy(storage_config)
    if proxy:
        disk_config["proxy"] = proxy


def _backup_storage_endpoint(storage_config: dict, key_prefix: str) -> str:
    """
    Build the backup storage URL of a given key prefix.

    "auto" addressing style is built as path-style, since it puts no
    requirements on the bucket name.
    """
    credentials = storage_config["credentials"]
    endpoint_url = credentials["endpoint_url"].rstrip("/")
    url = f"{endpoint_url}/{credentials['bucket']}/{key_prefix.strip('/')}/"

    if storage_config["boto_config"]["addressing_style"] == "virtual":
        return s3_uri_from_path_style_to_virtual_hosted(url)

    return url


def _backup_storage_proxy(storage_config: dict) -> dict | None:
    """
    Build the proxy configuration for reaching the backup storage, if any.

    ClickHouse is given the resolver rather than a host resolved once, so that
    it picks a proxy per request and drops one that fails. The endpoint always
    gets a path: the resolve request line is built out of the path alone.
    """
    proxy_resolver = storage_config.get("proxy_resolver", {})
    resolver_uri = proxy_resolver.get("uri")
    if not resolver_uri:
        return None

    endpoint = urlparse(resolver_uri)
    if not endpoint.path:
        endpoint = endpoint._replace(path="/")

    return {
        "resolver": {
            "endpoint": endpoint.geturl(),
            "proxy_scheme": "http",
            "proxy_port": str(proxy_resolver["proxy_port"]),
        }
    }


def _raise_request_timeout(disk_config: dict) -> bool:
    """
    Raise request timeout of a disk configuration, keeping a larger one intact.

    Returns whether the timeout was raised.
    """
    if int(disk_config.get("request_timeout_ms", 0)) >= (
        CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS
    ):
        return False

    disk_config["request_timeout_ms"] = str(CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS)
    return True


def _request_timeout_override() -> dict:
    """
    Build a configuration that raises request timeout of an already defined disk.
    """
    return {
        "request_timeout_ms": {
            "@replace": "replace",
            "#text": str(CH_OBJECT_STORAGE_REQUEST_TIMEOUT_MS),
        }
    }


def _remove_file(path: str) -> None:
    """
    Remove a file, ignoring a missing one.
    """
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


@contextmanager
def _open_config_file(path: str) -> Iterator[IO[str]]:
    """
    Open a config file for writing, replacing it atomically.

    ClickHouse and already running clickhouse-disks read these files while
    further disks are being added.
    """
    tmp_path = f"{path}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            yield f
        os.replace(tmp_path, path)
    except Exception:
        _remove_file(tmp_path)
        raise


def _render_disks_config(
    path: str, disks: dict, history_file: str | None = None
) -> None:
    """
    Write disks configuration as a ClickHouse config file.
    """
    config: dict[str, Any] = {"storage_configuration": {"disks": disks}}
    if history_file is not None:
        config["history-file"] = history_file

    with _open_config_file(path) as f:
        xmltodict.unparse(
            {"clickhouse": config},
            f,
            pretty=True,
        )


def _render_ch_disks_config(disks: dict[str, dict]) -> None:
    """
    Write configuration of the clickhouse-disks utility.
    """
    _render_disks_config(
        CH_DISK_CONFIG_PATH,
        {
            name: conf
            for name, conf in disks.items()
            if not conf or conf.get("type") != "cache"
        },
        history_file=CH_DISK_HISTORY_FILE_PATH,
    )


# pylint: disable=too-many-positional-arguments
def _ch_disks_copy(
    ch_ctl: ClickhouseCTL,
    from_disk: str,
    from_path: str,
    to_disk: str,
    to_path: str,
    routine_tag: str,
) -> None:
    """
    Copy a directory between disks with the clickhouse-disks utility.
    """
    command = "copy"
    common_args = ["--config", CH_DISK_CONFIG_PATH]
    if ch_ctl.ch_version_ge("24.7"):
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
    elif ch_ctl.ch_version_ge("23.9"):
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
    """
    Return path of the config file generated for a temporary disk.
    """
    return os.path.join(config_dir, f"cloud_storage_tmp_disk_{disk_name}.xml")


def _get_tmp_disk_name(disk_name: str, backup_name: str | None = None) -> str:
    """
    Return name of the temporary disk used to restore data of a given disk.

    A backup name is set for a disk reading data of another backup, where data
    of deduplicated parts is left.
    """
    if backup_name:
        return f"{disk_name}_source_{sanitize_backup_name(backup_name)}"

    return f"{disk_name}_source"


def _get_backup_disk_name(disk_name: str) -> str:
    """
    Return name of the temporary disk used to back up data of a given disk.
    """
    return f"{disk_name}_backup"


def _exec(
    routine_tag: str,
    exe: str,
    common_args: list[str],
    command: str | None = None,
    command_args: list[str] | None = None,
) -> Any:
    """
    Run a command and return its output lines.

    clickhouse-disks may report a failed command with zero exit code, writing
    only a line starting with "Error: " to stderr, so such a line fails the call.
    """
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
        errors = []
        for line in proc.stderr:  # type: ignore
            message = line.decode("utf-8").strip()
            proc_logger.info(message)
            if message.startswith("Error: "):
                errors.append(message)
        proc.wait()
        if proc.returncode != 0 or errors:
            raise ClickHouseDisksException(
                f"{exe} call failed with exitcode: {proc.returncode}, errors: {errors}"
            )

        return list(map(lambda b: b.decode("utf-8"), proc.stdout.readlines()))  # type: ignore
