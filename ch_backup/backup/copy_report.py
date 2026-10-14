"""Results of a backup copy run, collected in the parent process."""

import json
import os
import sys
from dataclasses import dataclass, field, fields
from itertools import islice
from tempfile import SpooledTemporaryFile
from typing import Iterator, Literal, TextIO

from ch_backup.exceptions import ClickhouseBackupError


class CopyErrors:
    """Append-only error details, spilling to disk above 64 KiB per backup."""

    def __init__(self) -> None:
        # The file lives as long as this report, including reads after copying raises.
        # pylint: disable-next=consider-using-with
        self._file = SpooledTemporaryFile(
            max_size=64 * 1024, mode="w+t", encoding="utf-8"
        )
        self._count = 0

    def append(self, detail: dict[str, str]) -> None:
        """Save one error without retaining its exception or traceback."""
        self._file.seek(0, os.SEEK_END)
        self._file.write(json.dumps(detail) + "\n")
        self._count += 1

    def __len__(self) -> int:
        return self._count

    def __iter__(self) -> Iterator[dict[str, str]]:
        # Each iterator has its own position; appends and other readers may seek.
        position = 0
        for _ in range(self._count):
            self._file.seek(position)
            line = self._file.readline()
            position = self._file.tell()
            yield json.loads(line)

    def __getitem__(self, index: int) -> dict[str, str]:
        if index < 0:
            index += self._count
        if not 0 <= index < self._count:
            raise IndexError(index)
        return next(islice(self, index, None))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (CopyErrors, list)):
            return NotImplemented
        return len(self) == len(other) and all(a == b for a, b in zip(self, other))


@dataclass
class BackupCopyResult:
    """Confirmed operations and errors for one backup."""

    name: str
    status: Literal["copied", "skipped", "failed", "not_processed"] = "not_processed"
    objects_total: int | None = None
    objects_copied: int = 0
    objects_failed: int = 0
    known_bytes_copied: int = 0
    errors: CopyErrors = field(default_factory=CopyErrors)

    def completed(self, size: int = 0) -> None:
        """Record a completed copy or metadata upload."""
        self.objects_copied += 1
        self.known_bytes_copied += size

    def add_error(
        self,
        error: BaseException,
        stage: str,
        *,
        source: str | None = None,
        target: str | None = None,
        object_failed: bool = False,
    ) -> None:
        """Record an error without retaining storage credentials or tracebacks."""
        detail = {"stage": stage, "message": f"{type(error).__name__}: {error}"}
        if source is not None:
            detail["source"] = source
        if target is not None:
            detail["target"] = target
        self.errors.append(detail)
        self.status = "failed"
        self.objects_failed += int(object_failed)


@dataclass
class CopyReport:
    """Report shared with callers even when copying raises an exception."""

    schema_version: int = 1
    error: str | None = None
    backups: list[BackupCopyResult] = field(default_factory=list)


def _write_report(report: CopyReport, report_file: TextIO) -> None:
    """Stream the existing JSON schema without copying all error details into RAM."""
    header = {"schema_version": report.schema_version, "error": report.error}
    report_file.write(json.dumps(header)[:-1] + ', "backups": [')
    for index, result in enumerate(report.backups):
        if index:
            report_file.write(",")
        values = {
            item.name: getattr(result, item.name)
            for item in fields(result)
            if item.name != "errors"
        }
        report_file.write("\n" + json.dumps(values)[:-1] + ', "errors": [')
        for error_index, error in enumerate(result.errors):
            if error_index:
                report_file.write(",")
            json.dump(error, report_file)
        report_file.write("]}")
    report_file.write("\n]}\n")


def print_copy_report(report: CopyReport, report_path: str | None) -> None:
    """Print confirmed results and optionally save them without masking copy errors."""
    statuses = ("copied", "skipped", "failed", "not_processed")
    for result in report.backups:
        suffix = (
            ", already present in target storage" if result.status == "skipped" else ""
        )
        print(f"{result.name}: {result.status}{suffix}")
        if result.objects_total is not None:
            print(
                f"  objects: {result.objects_copied}/{result.objects_total} copied, "
                f"{result.objects_failed} failed; known bytes copied: {result.known_bytes_copied}"
            )
        for error in result.errors:
            path = error.get("target", error.get("source", ""))
            print(f"  {error['stage']} {path}: {error['message']}")
    counts = [
        f"{status}: {sum(r.status == status for r in report.backups)}"
        for status in statuses
    ]
    print("Copy summary: " + ", ".join(counts), flush=True)
    if report.error:
        print(report.error, file=sys.stderr, flush=True)
    if report_path:
        try:
            # O_NOFOLLOW prevents writing through a symlink planted at the
            # report path, which could otherwise overwrite an arbitrary file.
            fd = os.open(
                report_path,
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW,
                0o644,
            )
            with os.fdopen(fd, "w", encoding="utf-8") as report_file:
                _write_report(report, report_file)
        except Exception as e:
            message = f"Failed to save copy report to {report_path}: {e}"
            print(message, file=sys.stderr, flush=True)
            if report.error is None:
                raise ClickhouseBackupError(message) from e
