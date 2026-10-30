"""Stage and publish complete local parts before ClickHouse can attach them."""

import os
import shutil
from dataclasses import dataclass
from hashlib import md5
from pathlib import Path
from tarfile import BLOCKSIZE
from types import TracebackType
from typing import Callable, Iterable
from uuid import uuid4

from ch_backup import logging
from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupMetadata, PartMetadata
from ch_backup.calculators import calc_aligned_files_size
from ch_backup.clickhouse.part_checksums import check_part_files
from ch_backup.exceptions import ClickhouseBackupError, is_broken_executor
from ch_backup.util import scan_dir_files


def part_staging_path(destination: str) -> str:
    """Choose a private sibling so publication cannot cross filesystems."""
    return os.path.join(os.path.dirname(destination), f".ch-backup-{uuid4().hex}")


def cleanup_staging_paths(paths: Iterable[str]) -> None:
    """Clean private paths after their writers finish, preserving the original error."""
    for path in paths:
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            pass
        except OSError:
            logging.warning(
                "Failed to remove restore staging directory {}", path, exc_info=True
            )


def promote_part(staging: str, destination: str) -> None:
    """Publish without replacing any preexisting destination, including symlinks."""
    if os.path.lexists(destination):
        raise ClickhouseBackupError(
            f'Detached part path already exists: "{destination}"'
        )
    os.rename(staging, destination)


def check_local_part(part: PartMetadata, path: str, *, existing: bool = False) -> None:
    """Check backup metadata; reusing existing data also requires its file hashes."""
    root = Path(path)
    if root.is_symlink() or not root.is_dir():
        raise ClickhouseBackupError(f'Expected a part directory: "{path}"')
    files = list(scan_dir_files(root, reject_symlinks=True))
    if sorted(files) != sorted(part.files):
        raise ClickhouseBackupError(f'Part file list does not match backup: "{path}"')
    paths = [root / name for name in files]
    # Per-file and early TAR backups stored unaligned sizes. Copying and
    # deduplication preserve those values, even in backups made by new versions.
    if (
        calc_aligned_files_size(paths, alignment=BLOCKSIZE) != part.size
        and calc_aligned_files_size(paths) != part.size
    ):
        raise ClickhouseBackupError(f'Part size does not match backup: "{path}"')
    if (
        not part.checksum
        or md5((root / "checksums.txt").read_bytes()).hexdigest() != part.checksum
    ):
        raise ClickhouseBackupError(f'Part checksum does not match backup: "{path}"')
    if existing:
        check_part_files(path, verify_compressed_hashes=True)


@dataclass
class _Download:
    part: PartMetadata
    destination: str
    staging: str | None
    complete: bool = False
    error: BaseException | None = None
    reported: bool = False

    def succeeded(self, _part: PartMetadata) -> None:
        """Record completion without publishing from an executor callback."""
        self.complete = True

    def failed(self, _part: PartMetadata, error: BaseException) -> None:
        """Remember a download error until every writer has stopped."""
        self.error = error


class PartDownloadStaging:
    """Keep staging private until its writers finish; retain it after fatal aborts."""

    def __init__(self, layout: BackupLayout) -> None:
        self._layout = layout
        self.paths: list[str] = []
        self._drained = False
        self._wait_started = False

    def __enter__(self) -> "PartDownloadStaging":
        return self

    def wait(self, keep_going: bool = False) -> None:
        """Record whether every writer finished before allowing cleanup."""
        self._wait_started = True
        self._layout.wait(keep_going=keep_going)
        self._drained = True

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if (
            not self._drained
            and exc_value is not None
            and (not isinstance(exc_value, Exception) or is_broken_executor(exc_value))
        ):
            # A signal during submission must not turn into an unbounded drain.
            # Forced shutdown is bounded; leave staging in case a writer survives.
            try:
                self._layout.abort()
            except Exception:
                logging.warning(
                    "Failed to stop part downloads while handling a fatal error",
                    exc_info=True,
                )
        elif not self._wait_started:
            try:
                self._layout.wait(keep_going=True)
                self._drained = True
            except BaseException as error:
                if (
                    exc_type is None
                    or not isinstance(error, Exception)
                    or (is_broken_executor(error) and isinstance(exc_value, Exception))
                ):
                    raise
                logging.warning(
                    "Failed to drain part downloads; retaining restore staging directories",
                    exc_info=True,
                )
        if self._drained:
            cleanup_staging_paths(self.paths)
        else:
            logging.warning(
                "Retaining restore staging directories because downloads did not drain"
            )


class LocalPartRestorer:
    """Own a table's in-flight local downloads and publish only confirmed parts.

    Drain ordinary download failures before cleanup, even without keep-going: a
    failed legacy part can still have other file writers running in the pool.
    Fatal executor/signal failures leave private directories for manual cleanup.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        layout: BackupLayout,
        backup: BackupMetadata,
        *,
        keep_going: bool,
        on_success: Callable[[PartMetadata], None],
        on_error: Callable[[PartMetadata, Exception], None],
        check_data: bool = False,
    ) -> None:
        self._layout = layout
        self._backup = backup
        self._keep_going = keep_going
        self._on_success = on_success
        self._on_error = on_error
        self._check_data = check_data
        self._downloads: list[_Download] = []
        self._staging = PartDownloadStaging(layout)

    def download(self, part: PartMetadata, destination: str) -> None:
        """Schedule a fresh download or defer validation of an occupied path."""
        download = _Download(
            part,
            destination,
            None if os.path.lexists(destination) else part_staging_path(destination),
        )
        self._downloads.append(download)
        if download.staging is None:
            return
        self._staging.paths.append(download.staging)
        try:
            self._layout.download_data_part(
                self._backup,
                part,
                download.staging,
                callback=download.succeeded,
                on_error=download.failed,
            )
        except Exception as error:
            download.error = error
            download.reported = True
            self._on_error(part, error)
            if not self._keep_going or is_broken_executor(error):
                raise

    def __enter__(self) -> "LocalPartRestorer":
        return self

    def wait(self) -> None:
        """Drain files, validate and publish; success callbacks run after rename."""
        self._staging.wait(keep_going=True)
        for download in self._downloads:
            if download.reported:
                continue
            try:
                if download.error is not None:
                    raise download.error
                if download.staging is not None and not download.complete:
                    raise ClickhouseBackupError("Part download was not confirmed")
                check_local_part(
                    download.part,
                    download.staging or download.destination,
                    existing=download.staging is None,
                )
                if download.staging:
                    if self._check_data:
                        check_part_files(download.staging)
                    promote_part(download.staging, download.destination)
            except Exception as error:
                self._on_error(download.part, error)
                if not self._keep_going or is_broken_executor(error):
                    raise
                continue
            # State persistence errors are fatal, not a failed download to skip.
            self._on_success(download.part)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._staging.__exit__(exc_type, exc_value, traceback)
