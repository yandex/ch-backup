"""Faults that must run inside the ClickHouse container."""

import argparse
import shutil
import subprocess
from collections import Counter
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import cast
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from botocore.awsrequest import AWSResponse
from click import Command
from urllib3.response import HTTPResponse

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupState
from ch_backup.cli import cli
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.clickhouse.disks import (
    ClickHouseDisksException,
    ClickHouseTemporaryDisks,
)
from ch_backup.clickhouse.object_storage_metadata import parse_object_storage_metadata
from ch_backup.config import Config
from ch_backup.storage.engine.s3 import S3RetryingError, S3RetryPolicy, S3StorageEngine


def _run_patched_cli(arguments: list[str]) -> None:
    # Keep storage work in this process: spawned workers cannot import the
    # test module after CLI setup changes its working directory.
    cast(Command, cli).main(
        ["--config-parameter", "multiprocessing.workers", "0", *arguments]
    )


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


def retry_multipart_copy(attempts: int, config_path: str) -> None:
    """Inject one HTTP response, retaining SDK retries and real concurrent S3 I/O."""
    config = Config(config_path).derive(
        patch={
            "storage": {
                "retry_max_attempts": 1,
                "module_config": {
                    "botocore": {"retries": {"total_max_attempts": attempts}}
                },
            }
        }
    )
    engine = S3StorageEngine(config["storage"], retry_policy=S3RetryPolicy.DEFAULT)
    source, target = "module-config/source", f"module-config/copy-{attempts}"
    data = bytes(range(256)) * (12 * 1024**2 // 256)
    engine.upload_data(data, source)
    client = getattr(engine, "_s3_client")
    requests: Counter = Counter()
    ranges: set[str] = set()
    lock = Lock()

    def fail_first_part(request, **_kwargs):
        query = parse_qs(urlparse(request.url).query)
        upload_id, part = query["uploadId"][0], query["partNumber"][0]
        with lock:
            requests[(upload_id, part)] += 1
            ranges.add(request.headers["x-amz-copy-source-range"].decode())
            fail = part == "1" and requests[(upload_id, part)] == 1
        if fail:
            body = b"<Error><Code>InternalError</Code><Message>injected retryable error</Message></Error>"
            return AWSResponse(
                request.url,
                500,
                {"content-type": "application/xml"},
                HTTPResponse(BytesIO(body), preload_content=False),
            )
        return None

    client.meta.events.register("before-send.s3.UploadPartCopy", fail_first_part)
    try:
        engine.copy_object(config["storage"]["credentials"]["bucket"], source, target)
    except S3RetryingError:
        assert attempts == 1, "SDK retries did not recover multipart copy"
        assert not engine.path_exists(
            target
        ), "Failed copy published a destination object"
    else:
        assert attempts > 1, "Copy unexpectedly succeeded without SDK retries"
        assert engine.download_data(target) == data
        assert ranges == {
            "bytes=0-5242879",
            "bytes=5242880-10485759",
            "bytes=10485760-12582911",
        }, ranges
    assert len({upload_id for upload_id, _ in requests}) == 1, requests
    assert sum(count for (_, part), count in requests.items() if part == "1") == (
        2 if attempts > 1 else 1
    ), requests
    print("Multipart copy retry and contents verified")


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
    multipart_copy = subparsers.add_parser("retry-multipart-copy")
    multipart_copy.add_argument("attempts", type=int)
    multipart_copy.add_argument("config_path")
    options = parser.parse_args()
    if options.fault == "corrupt-file":
        corrupt_file(options.path, options.action)
    elif options.fault == "fail-backup-publication":
        fail_backup_publication(options.backup_name, options.arguments)
    elif options.fault == "drop-frozen-table":
        drop_frozen_table(options.database, options.table, options.arguments)
    elif options.fault == "interrupt-inplace-copy":
        interrupt_inplace_copy(options.filename, options.arguments)
    elif options.fault == "retry-multipart-copy":
        retry_multipart_copy(options.attempts, options.config_path)
    else:
        fail_part_attach(options.database, options.table, options.arguments)
