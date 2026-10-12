"""Results of a backup copy run, collected in the parent process."""

import json
import os
import sys
from dataclasses import asdict, dataclass, field
from typing import Literal

from ch_backup.exceptions import ClickhouseBackupError


@dataclass
class BackupCopyResult:
    """Confirmed operations and errors for one backup."""

    name: str
    status: Literal["copied", "skipped", "failed", "not_processed"] = "not_processed"
    objects_total: int | None = None
    objects_copied: int = 0
    objects_failed: int = 0
    known_bytes_copied: int = 0
    errors: list[dict[str, str]] = field(default_factory=list)

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
                report_file.write(json.dumps(asdict(report), indent=2) + "\n")
        except Exception as e:
            message = f"Failed to save copy report to {report_path}: {e}"
            print(message, file=sys.stderr, flush=True)
            if report.error is None:
                raise ClickhouseBackupError(message) from e
