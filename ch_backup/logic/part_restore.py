"""Publish local parts only after their downloads and metadata checks succeed."""

import os
import shutil
from errno import ENOTEMPTY
from hashlib import md5
from pathlib import Path
from tarfile import BLOCKSIZE
from typing import Callable

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.backup.metadata.table_metadata import split_part_name
from ch_backup.calculators import calc_aligned_files_size
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import LOCAL_RESTORE_DIR, scan_dir_files


def check_local_part(part: PartMetadata, path: str) -> None:
    """Compare downloaded files with the file list, size and metadata checksum."""
    root = Path(path)
    files = list(scan_dir_files(root))
    if sorted(files) != sorted(part.files):
        raise ClickhouseBackupError(f'Part file list does not match backup: "{path}"')
    paths = [root / name for name in files]
    # Old per-file and early TAR backups recorded unaligned sizes, which are
    # also preserved when those parts are copied or linked by a newer backup.
    if (
        calc_aligned_files_size(paths, alignment=BLOCKSIZE) != part.size
        and calc_aligned_files_size(paths) != part.size
    ):
        raise ClickhouseBackupError(f'Part size does not match backup: "{path}"')
    if md5((root / "checksums.txt").read_bytes()).hexdigest() != part.checksum:
        raise ClickhouseBackupError(f'Part checksum does not match backup: "{path}"')


def _remove_part(path: Path) -> None:
    if path.is_symlink():
        path.unlink()
    else:
        shutil.rmtree(path)


def _discard_downloads(root: Path) -> None:
    try:
        (root / "previous").rmdir()
    except FileNotFoundError:
        pass
    except OSError as error:
        if error.errno != ENOTEMPTY:
            raise
        # Keep the entire rename state for recovery, including incoming if an
        # unexpected destination prevented both publication and rollback.
        return
    incoming = root / "incoming"
    if incoming.exists():
        shutil.rmtree(incoming)
    try:
        root.rmdir()
    except FileNotFoundError:
        pass


def _recover_workspace(root: Path) -> None:
    """Reconcile the two publication renames before accepting new writes."""
    if root.parent.is_symlink():
        raise ClickhouseBackupError(
            f"Unexpected symlink to detached directory: {root.parent}"
        )
    if not os.path.lexists(root):
        return
    if root.is_symlink() or not root.is_dir():
        raise ClickhouseBackupError(f"Unexpected restore workspace: {root}")

    incoming, previous = root / "incoming", root / "previous"
    # Inspect the complete workspace before modifying it. A previous part
    # may itself be a symlink if the old detached destination was one.
    for directory in root.iterdir():
        if (
            directory not in (incoming, previous)
            or directory.is_symlink()
            or not directory.is_dir()
        ):
            raise ClickhouseBackupError(
                f"Unexpected restore workspace entry: {directory}"
            )
        for entry in directory.iterdir():
            split_part_name(entry.name)
            if entry.is_symlink() and directory == previous:
                continue
            if entry.is_symlink() or not entry.is_dir():
                raise ClickhouseBackupError(f"Unexpected staged part: {entry}")

    old_parts = list(previous.iterdir()) if previous.exists() else []
    for old in old_parts:
        destination = root.parent / old.name
        if os.path.lexists(destination) and (
            (incoming / old.name).exists()
            or not (destination.is_symlink() or destination.is_dir())
        ):
            raise ClickhouseBackupError(
                f"Conflicting restore paths for {destination} in {root}"
            )
    for old in old_parts:
        destination = root.parent / old.name
        if os.path.lexists(destination):
            _remove_part(old)
        else:
            os.rename(old, destination)
    _discard_downloads(root)


def _publish_local_part(staging: Path, destination: Path) -> None:
    """Publish the validated part, retaining the old directory until rename succeeds."""
    if not (destination.is_symlink() or destination.is_dir()):
        os.rename(staging, destination)
        return

    previous = destination.parent / LOCAL_RESTORE_DIR / "previous" / destination.name
    previous.parent.mkdir(exist_ok=True)
    os.rename(destination, previous)
    try:
        os.rename(staging, destination)
    except BaseException:
        try:
            os.rename(previous, destination)
        except OSError as error:
            logging.error(
                "Rollback of {} failed: {}. Previous part retained at {}",
                destination,
                error,
                previous,
            )
        raise

    _remove_part(previous)


class LocalPartRestorer:
    """Stage a table's downloads and publish successful parts after workers finish."""

    def __init__(
        self,
        layout: BackupLayout,
        backup: BackupMetadata,
        *,
        keep_going: bool,
        on_success: Callable[[PartMetadata], None],
        on_error: Callable[[PartMetadata, BaseException], None],
    ) -> None:
        self._layout = layout
        self._backup = backup
        self._keep_going = keep_going
        self._on_success = on_success
        self._on_error = on_error
        self._roots: set[Path] = set()
        self._completed: list[tuple[PartMetadata, Path, Path]] = []

    def prepare(self, detached: Path) -> None:
        """Recover once per target path, including disks with only restored parts."""
        root = detached / LOCAL_RESTORE_DIR
        if root not in self._roots:
            _recover_workspace(root)
            self._roots.add(root)

    def download(self, part: PartMetadata, destination: str) -> None:
        """Schedule a download into the recovered workspace on the target disk."""
        final = Path(destination)
        self.prepare(final.parent)
        root = final.parent / LOCAL_RESTORE_DIR
        staging = root / "incoming" / final.name
        staging.parent.mkdir(parents=True, exist_ok=True)

        def downloaded(_part: PartMetadata) -> None:
            self._completed.append((part, staging, final))

        self._layout.download_data_part(
            self._backup,
            part,
            str(staging),
            callback=downloaded,
            on_error=self._on_error,
        )

    def wait(self) -> None:
        """Validate and replace destinations before recording DOWNLOADED."""
        # A failed wait can leave writers running. Keep their private directories;
        # only a successful drain (including keep-going) makes cleanup safe.
        self._layout.wait(self._keep_going)
        try:
            for part, staging, destination in self._completed:
                try:
                    check_local_part(part, str(staging))
                    _publish_local_part(staging, destination)
                except Exception as error:
                    self._on_error(part, error)
                    if not self._keep_going:
                        raise
                    continue
                # A state persistence failure must propagate even with keep-going.
                self._on_success(part)
        finally:
            for root in self._roots:
                _discard_downloads(root)
