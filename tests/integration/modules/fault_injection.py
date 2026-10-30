"""Faults that must run inside the ClickHouse container."""

import argparse
import shutil
import subprocess
import sys
import time
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast
from unittest.mock import patch

from click import Command

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupState
from ch_backup.cli import cli
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.disks import (
    ClickHouseDisksException,
    ClickHouseTemporaryDisks,
)
from ch_backup.clickhouse.object_storage_metadata import parse_object_storage_metadata
from ch_backup.storage.async_pipeline.pipeline_executor import PipelineExecutor
from ch_backup.storage.async_pipeline.pipelines import download_file_pipeline


def _run_patched_cli(arguments: list[str], workers: int | None = 0) -> None:
    # CLI setup changes cwd. Keep test helpers importable by spawned workers.
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    worker_options = (
        []
        if workers is None
        else ["--config-parameter", "multiprocessing.workers", str(workers)]
    )
    cast(Command, cli).main([*worker_options, *arguments])


def _download_with_failure(
    pipeline: partial, release: Path, completed: Path, delayed: bool
) -> None:
    """Run real S3 I/O, keeping one writer alive until the error is observed."""
    if delayed:
        deadline = time.monotonic() + 30
        while not release.exists():
            assert time.monotonic() < deadline, "Download error was not observed"
            time.sleep(0.01)
        assert pipeline.args[2].parent.is_dir(), "Staging was removed before draining"
    pipeline()
    if delayed:
        completed.touch()
    else:
        raise OSError("injected part file download failure")


def fail_part_download(phase: str, arguments: list[str]) -> None:
    """Fail a future or submission while a sibling file still needs to be written."""
    # pylint: disable=protected-access
    execute = PipelineExecutor._exec_pipeline
    with TemporaryDirectory(prefix="ch-backup-download-fault-") as directory:
        # The CLI drops root privileges before starting transfer workers.
        shutil.chown(directory, user="clickhouse", group="clickhouse")
        release, completed = Path(directory) / "release", Path(directory) / "completed"

        def submit(executor, job_id, pipeline, is_async, callback=None, on_error=None):
            # pylint: disable=too-many-positional-arguments
            if (
                pipeline.func is download_file_pipeline
                and "/legacy_async/" in pipeline.args[1]
            ):
                assert (
                    executor._exec_pool is not None
                ), "The fault requires real workers"
                name = pipeline.args[2].name
                if phase == "submission" and name == "checksums.txt":
                    release.touch()
                    raise OSError("injected part file submission failure")
                if name in {"count.txt", "checksums.txt"}:

                    def failed(error, handler=on_error):
                        release.touch()
                        assert handler is not None
                        handler(error)

                    pipeline = partial(
                        _download_with_failure,
                        pipeline,
                        release,
                        completed,
                        phase == "submission" or name == "checksums.txt",
                    )
                    on_error = failed if phase == "worker" else on_error
            return execute(executor, job_id, pipeline, is_async, callback, on_error)

        with patch.object(PipelineExecutor, "_exec_pipeline", submit):
            try:
                _run_patched_cli(arguments, workers=None)
            finally:
                assert completed.exists(), "The pending file writer did not finish"
                print("Pending part file download finished before cleanup")


def corrupt_file(path: str, action: str) -> None:
    """Modify the existing inode, including any FREEZE hard links."""
    with open(path, "r+b") as stream:
        first = stream.read(1)
        assert first, f"Cannot corrupt an empty file: {path}"
        if action == "flip_byte":
            stream.seek(0)
            stream.write(bytes([first[0] ^ 8]))
        else:
            assert action == "truncate", action
            stream.seek(0, 2)
            stream.truncate(stream.tell() - 1)


def fail_backup_publication(backup_name: str, arguments: list[str]) -> None:
    """Fail only the final publication; let copy persist FAILED metadata."""
    upload = BackupLayout.upload_backup_metadata

    def fail_owner(layout, backup, *args, **kwargs):
        if backup.name == backup_name and backup.state == BackupState.CREATED:
            raise RuntimeError("injected owner publication failure")
        return upload(layout, backup, *args, **kwargs)

    with patch.object(BackupLayout, "upload_backup_metadata", fail_owner):
        _run_patched_cli(arguments)


def drop_frozen_table(database: str, table_name: str, arguments: list[str]) -> None:
    """Remove the original table after FREEZE, before its metadata is uploaded."""
    upload = BackupLayout.upload_cloud_storage_metadata

    def upload_after_drop(layout, backup, disk, table, *args, **kwargs):
        if (table.database, table.name) == (database, table_name):
            subprocess.run(
                [
                    "clickhouse-client",
                    "--query",
                    f"DROP TABLE IF EXISTS `{database}`.`{table_name}` SYNC",
                ],
                check=True,
            )
        return upload(layout, backup, disk, table, *args, **kwargs)

    with patch.object(BackupLayout, "upload_cloud_storage_metadata", upload_after_drop):
        _run_patched_cli(arguments)


def interrupt_inplace_copy(filename: str, arguments: list[str]) -> None:
    """Leave one real metadata file, then fail before the part is DOWNLOADED."""

    # pylint: disable=too-many-positional-arguments
    def interrupted_copy(manager, from_disk, from_path, to_disk, to_path, _tag):
        # pylint: disable=protected-access
        source = Path(manager._ch_ctl.get_disk(from_disk).path) / from_path / filename
        target = Path(manager._ch_ctl.get_disk(to_disk).path) / to_path
        target.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target / filename)
        metadata = parse_object_storage_metadata(source.read_bytes())
        raise ClickHouseDisksException(
            f"injected inplace copy interruption; ref_count={metadata.ref_count}"
        )

    with patch.object(ClickHouseTemporaryDisks, "_os_copy", interrupted_copy):
        _run_patched_cli(arguments)


def fail_part_attach(database: str, table_name: str, arguments: list[str]) -> None:
    """Let copying finish, then fail before ATTACH consumes the detached part."""
    attach = ClickhouseCTL.attach_part

    def fail_attach(controller, table, part_name):
        if (table.database, table.name) == (database, table_name):
            raise RuntimeError("injected ATTACH failure after copying")
        return attach(controller, table, part_name)

    with patch.object(ClickhouseCTL, "attach_part", fail_attach):
        _run_patched_cli(arguments)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="fault", required=True)
    corruption = subparsers.add_parser("corrupt-file")
    corruption.add_argument("path")
    corruption.add_argument("action", choices=["flip_byte", "truncate"])
    publication = subparsers.add_parser("fail-backup-publication")
    publication.add_argument("backup_name")
    publication.add_argument("arguments", nargs=argparse.REMAINDER)
    drop_table = subparsers.add_parser("drop-frozen-table")
    drop_table.add_argument("database")
    drop_table.add_argument("table")
    drop_table.add_argument("arguments", nargs=argparse.REMAINDER)
    inplace = subparsers.add_parser("interrupt-inplace-copy")
    inplace.add_argument("filename")
    inplace.add_argument("arguments", nargs=argparse.REMAINDER)
    attach_part = subparsers.add_parser("fail-part-attach")
    attach_part.add_argument("database")
    attach_part.add_argument("table")
    attach_part.add_argument("arguments", nargs=argparse.REMAINDER)
    download = subparsers.add_parser("fail-part-download")
    download.add_argument("phase", choices=["worker", "submission"])
    download.add_argument("arguments", nargs=argparse.REMAINDER)
    options = parser.parse_args()
    if options.fault == "corrupt-file":
        corrupt_file(options.path, options.action)
    elif options.fault == "fail-backup-publication":
        fail_backup_publication(options.backup_name, options.arguments)
    elif options.fault == "drop-frozen-table":
        drop_frozen_table(options.database, options.table, options.arguments)
    elif options.fault == "interrupt-inplace-copy":
        interrupt_inplace_copy(options.filename, options.arguments)
    elif options.fault == "fail-part-download":
        fail_part_download(options.phase, options.arguments)
    else:
        fail_part_attach(options.database, options.table, options.arguments)
