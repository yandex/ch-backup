"""Target-local part ownership for an independent backup-copy window."""

from collections import defaultdict
from datetime import datetime
from hashlib import blake2b
from typing import NamedTuple

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import (
    BackupMetadata,
    BackupState,
    PartMetadata,
    split_part_name,
)
from ch_backup.exceptions import is_broken_executor

PartsByTable = dict[tuple[str, str], list[tuple[tuple, PartMetadata]]]


class _StoredPart(NamedTuple):
    backup_name: str
    start_time: datetime
    part_name: str


def _part_key(part: PartMetadata) -> tuple:
    # Use the same parser/rule as backup deduplication: only a mutation suffix may differ.
    name: tuple[str, int, int, int] | str
    try:
        name = split_part_name(part.name)[:4]
    except ValueError:
        name = part.name
    return (
        part.database,
        part.table,
        name,
        part.checksum,
        part.disk_name,
        part.size,
        part.tarball,
        part.encrypted,
        # File names cannot contain NUL. Preserve file-list matching without
        # retaining every name after the candidate metadata is released.
        blake2b("\0".join(sorted(part.files)).encode(), digest_size=16).digest(),
    )


def _local_parts(backup: BackupMetadata) -> PartsByTable:
    """Parse the entire candidate before indexing it or accessing its data objects."""
    parts: PartsByTable = defaultdict(list)
    for part in backup.get_parts():
        # Legacy per-file metadata has no individual sizes. Matching file names
        # cannot verify an existing owner, so materialize those parts from source.
        if (
            part.tarball
            and part.link is None
            and part.disk_name not in backup.cloud_storage.disks
        ):
            parts[(part.database, part.table)].append((_part_key(part), part))
    return parts


class CopyDeduplication:
    """Index verified physical local parts, without retaining complete backup metadata."""

    def __init__(
        self, layout: BackupLayout, cutoff: datetime, command_start: datetime
    ) -> None:
        self._layout = layout
        self.cutoff = cutoff
        self.command_start = command_start
        self._parts: dict[tuple, list[_StoredPart]] = defaultdict(list)
        self._backup_keys: dict[str, set[tuple]] = defaultdict(set)

    def eligible(self, backup: BackupMetadata) -> bool:
        """A fresh reference cannot extend the physical owner's window."""
        return (
            backup.start_time is not None
            and self.cutoff <= backup.start_time <= self.command_start
            and backup.state in (BackupState.CREATED, BackupState.PARTIALLY_DELETED)
            and not backup.schema_only
        )

    def load(self, backups: list[BackupMetadata]) -> None:
        """Read each eligible full metadata once; an optional unreadable candidate is skipped."""
        for summary in backups:
            try:
                backup = self._layout.get_backup(summary.name)
                if (
                    backup is None
                    or backup.name != summary.name
                    or not self.eligible(backup)
                ):
                    continue
                parts = _local_parts(backup)
            except Exception as exc:
                if is_broken_executor(exc):
                    raise
                logging.warning(
                    "Cannot read copy deduplication candidate {}, skipping it ({})",
                    summary.name,
                    type(exc).__name__,
                )
                continue
            # Storage/executor errors in the shared inventory operation stay fatal.
            self._index_parts(backup, parts, published=False)
            del backup, parts

    def add_backup(self, backup: BackupMetadata, *, published: bool = False) -> None:
        """New owners are trusted only after their data and CREATED metadata are published."""
        if not self.eligible(backup):
            return
        self._index_parts(backup, _local_parts(backup), published=published)

    def _index_parts(
        self, backup: BackupMetadata, tables: PartsByTable, *, published: bool
    ) -> None:
        for (database, table), parts in tables.items():
            # List once per table, release its file inventory before moving to the next.
            files = (
                {}
                if published
                else self._layout.get_table_data_files(backup.name, database, table)
            )
            for key, part in parts:
                if not published and not self._layout.check_data_part_files(
                    part, files.get(part.name, {})
                ):
                    continue
                candidates = self._parts[key]
                # Only PARTIALLY_DELETED owners can be restarted in this command;
                # existing CREATED targets and newly published owners are skipped.
                if backup.state == BackupState.PARTIALLY_DELETED:
                    self._backup_keys[backup.name].add(key)
                candidates.append(
                    _StoredPart(backup.name, backup.start_time, part.name)
                )
                candidates.sort(
                    key=lambda item: (
                        item.start_time,
                        item.backup_name,
                        item.part_name,
                    ),
                    reverse=True,
                )
            del files

    def discard_backup(self, backup_name: str) -> None:
        """A restarted owner becomes unavailable before any of its metadata is rewritten."""
        for key in self._backup_keys.pop(backup_name, ()):
            remaining = [
                part for part in self._parts[key] if part.backup_name != backup_name
            ]
            if remaining:
                self._parts[key] = remaining
            else:
                del self._parts[key]

    def find_part(self, part: PartMetadata, backup_name: str) -> _StoredPart | None:
        """Return the newest compatible physical owner other than the current backup."""
        return next(
            (
                candidate
                for candidate in self._parts.get(_part_key(part), ())
                if candidate.backup_name != backup_name
            ),
            None,
        )
