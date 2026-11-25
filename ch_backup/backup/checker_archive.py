"""Read shared cloud disk metadata archives for backup checks."""

import threading
from contextlib import closing
from typing import Callable, NamedTuple

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, TableMetadata
from ch_backup.backup.metadata.cloud_storage_metadata import CloudStorageBackup
from ch_backup.clickhouse.object_storage_metadata import FROZEN_METADATA_FNAME
from ch_backup.util import escape_metadata_file_name


class CloudFile(NamedTuple):
    """Parsed references or diagnostic for one file of cloud disk metadata."""

    objects: set[tuple[str, int]]
    problem: str | None = None


class LegacyDisk:
    """Hold only parsed files required by checks sharing a legacy disk archive."""

    def __init__(self) -> None:
        """Create shared state for the disk and its archive readers."""
        self.lock = threading.Lock()
        self.parts: set[tuple[str, str]] = set()
        self.archives: dict[
            tuple[str, bool], dict[tuple[str, str], dict[str, CloudFile]] | str
        ] = {}
        self.failure: str | None = None


def cloud_table_paths(table: TableMetadata) -> set[str]:
    """
    Return possible paths of a table on disk, as ClickHouse lays them out.
    """
    paths = {
        f"data/{escape_metadata_file_name(table.database)}/"
        f"{escape_metadata_file_name(table.name)}"
    }
    if table.uuid:
        paths.add(f"store/{table.uuid[:3]}/{table.uuid}")
    return paths


def cloud_part_member(name: str) -> tuple[str, str, str] | None:
    """Split a stored ClickHouse file path into table directory, part and filename."""
    path = name.split("/", 4)
    if len(path) != 5:
        return None
    return "/".join(path[:3]), path[3], path[4]


def files_of_parts(
    files: list[tuple[str, bytes]], parts: set[str], table_paths: set[str]
) -> dict[str, dict[str, bytes]]:
    """Select files under the requested table directories and parts."""
    result: dict[str, dict[str, bytes]] = {}
    for name, data in files:
        member = cloud_part_member(name)
        if member is None:
            continue
        table, part, filename = member
        if table in table_paths and part in parts:
            members = result.setdefault(part, {})
            if filename.rsplit("/", 1)[-1] != FROZEN_METADATA_FNAME:
                members[filename] = data
    return result


def read_legacy_files(
    layout: BackupLayout,
    backup: BackupMetadata | CloudStorageBackup,
    remote_path: str,
    prefix: str,
    parts: set[tuple[str, str]],
    *,
    parse_file: Callable[[bytes, str], CloudFile],
) -> dict[tuple[str, str], dict[str, CloudFile]]:
    """Read bounded batches and keep references for the requested parts."""
    found: dict[tuple[str, str], dict[str, CloudFile]] = {}
    with closing(
        layout.download_cloud_storage_metadata_archive(backup, remote_path)
    ) as archive:
        continuation = None
        while True:
            batch, continuation = archive.read_batch(continuation)
            for name, data in batch:
                member = cloud_part_member(name)
                if member is None:
                    continue
                table, part, filename = member
                if (table, part) in parts:
                    members = found.setdefault((table, part), {})
                    if filename.rsplit("/", 1)[-1] != FROZEN_METADATA_FNAME:
                        members[filename] = parse_file(data, prefix)
            if continuation is None:
                return found
