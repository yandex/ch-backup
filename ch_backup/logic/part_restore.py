"""Publish local parts only after their downloads and metadata checks succeed."""

import os
import shutil
from hashlib import md5
from pathlib import Path
from tarfile import BLOCKSIZE
from typing import Callable

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata, split_part_name
from ch_backup.calculators import calc_aligned_files_size
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import scan_dir_files

LOCAL_RESTORE_DIR = ".ch-backup-restore"


def is_local_restore_workspace(root: Path) -> bool:
    """Recognize the reserved staging layout, even before it contains parts."""
    if root.name != LOCAL_RESTORE_DIR or root.is_symlink() or not root.is_dir():
        return False
    for directory in root.iterdir():
        if (
            directory.name != "incoming"
            or directory.is_symlink()
            or not directory.is_dir()
        ):
            return False
        for entry in directory.iterdir():
            try:
                split_part_name(entry.name)
            except ValueError:
                return False
            if entry.is_symlink() or not entry.is_dir():
                return False
    return True


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


def _clear_workspace(root: Path) -> None:
    """Discard staged downloads after verifying the workspace belongs to restore."""
    if root.parent.is_symlink():
        raise ClickhouseBackupError(
            f"Unexpected symlink to detached directory: {root.parent}"
        )
    if not os.path.lexists(root):
        return
    if not is_local_restore_workspace(root):
        raise ClickhouseBackupError(
            f"Unexpected contents at restore workspace path {root}. "
            "Inspect the path and move any user-detached part aside before retrying."
        )

    shutil.rmtree(root)


def _publish_local_part(staging: Path, destination: Path) -> None:
    """Remove the old detached part and publish the validated download."""
    if destination.is_symlink():
        destination.unlink()
    elif destination.is_dir():
        shutil.rmtree(destination)
    os.rename(staging, destination)


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
        """Clear abandoned downloads once per target path, including disks with only restored parts."""
        root = detached / LOCAL_RESTORE_DIR
        if root not in self._roots:
            _clear_workspace(root)
            self._roots.add(root)

    def download(self, part: PartMetadata, destination: str) -> None:
        """Schedule a download into the workspace on the target disk."""
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
                _clear_workspace(root)
