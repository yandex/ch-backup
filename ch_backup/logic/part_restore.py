"""Publish local parts only after their downloads and metadata checks succeed."""

import os
import shutil
from hashlib import md5
from pathlib import Path
from tarfile import BLOCKSIZE
from typing import Callable
from uuid import uuid4

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.calculators import calc_aligned_files_size
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.util import scan_dir_files


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


def _remove_staging(path: str) -> None:
    try:
        if os.path.islink(path):
            os.unlink(path)
        else:
            shutil.rmtree(path)
    except FileNotFoundError:
        pass
    except OSError as error:
        logging.warning("Failed to remove restore staging path {}: {}", path, error)


def _publish_local_part(staging: str, destination: str) -> None:
    """Publish the validated part, retaining the old directory until rename succeeds."""
    if not (os.path.islink(destination) or os.path.isdir(destination)):
        os.rename(staging, destination)
        return

    previous = f"{staging}.previous"
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

    _remove_staging(previous)


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
        self._paths: list[str] = []
        self._completed: list[tuple[PartMetadata, str, str]] = []

    def download(self, part: PartMetadata, destination: str) -> None:
        """Download into a unique sibling of the final directory on the same disk."""
        staging = os.path.join(
            os.path.dirname(destination), f".ch-backup-{uuid4().hex}"
        )
        self._paths.append(staging)

        def downloaded(_part: PartMetadata) -> None:
            self._completed.append((part, staging, destination))

        self._layout.download_data_part(
            self._backup, part, staging, callback=downloaded, on_error=self._on_error
        )

    def wait(self) -> None:
        """Validate and replace destinations before recording DOWNLOADED."""
        # A failed wait can leave writers running. Keep their private directories;
        # only a successful drain (including keep-going) makes cleanup safe.
        self._layout.wait(self._keep_going)
        try:
            for part, staging, destination in self._completed:
                try:
                    check_local_part(part, staging)
                    _publish_local_part(staging, destination)
                except Exception as error:
                    self._on_error(part, error)
                    if not self._keep_going:
                        raise
                    continue
                # A state persistence failure must propagate even with keep-going.
                self._on_success(part)
        finally:
            for path in self._paths:
                _remove_staging(path)
