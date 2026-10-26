"""Faults that must run inside the ClickHouse container."""

import argparse
import shutil
import socket
import subprocess
from pathlib import Path
from threading import Event
from typing import cast
from unittest.mock import patch
from uuid import uuid4

from click import Command
from kazoo.protocol.serialization import Auth

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
from ch_backup.zookeeper.zookeeper import ZookeeperClient


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


def check_zookeeper_reconnect(config_file: str) -> None:
    """Drop a real auth connection and check recovery, nested locks and cleanup."""
    # Kazoo internals are used only to place the disconnect precisely: the
    # real socket, network thread, reconnect loop and public client API run.
    # pylint: disable=protected-access
    config = Config(config_file)["zookeeper"]
    wrapper = ZookeeperClient(config)
    connection = wrapper._client._connection
    send_request = connection._send_request
    connect = connection._connect
    dropped = Event()
    path = f"/ch-backup-reconnect-{uuid4().hex}"

    def drop_auth(read_timeout, connect_timeout):
        if connection.client._queue:
            request, _ = connection.client._queue[0]
            if isinstance(request, Auth) and not dropped.is_set():
                dropped.set()
                connection._socket.shutdown(socket.SHUT_RDWR)
        return send_request(read_timeout, connect_timeout)

    def delayed_connect(*args):
        if dropped.is_set():
            # Let the outer retry run while Kazoo is still reconnecting.
            # Correct cleanup interrupts this wait through Kazoo's stop flag.
            connection.client._stopped.wait(1)
        return connect(*args)

    with ZookeeperClient(config) as observer:
        with (
            patch.object(connection, "_send_request", drop_auth),
            patch.object(connection, "_connect", delayed_connect),
        ):
            with wrapper as client:
                assert dropped.is_set(), "The auth connection was not interrupted"
                session = client.client_id
                with client.Lock(path):
                    with wrapper as nested:
                        assert nested is client
                        nested.create(path + "/data", b"recovered")
                    assert client.client_id == session
                    contender = observer.Lock(path)
                    assert not contender.acquire(
                        blocking=False
                    ), "Nested exit released the lock"
                    assert observer.get(path + "/data")[0] == b"recovered"
            assert not client.connected
            assert connection._connection_routine is None
            assert connection.connection_closed.is_set()
            # close() must leave the wrapper reusable for a later context.
            with wrapper as client:
                assert client.get(path + "/data")[0] == b"recovered"
                with client.Lock(path):
                    client.set(path + "/data", b"reentered")
            assert observer.get(path + "/data")[0] == b"reentered"
            assert connection._connection_routine is None
            assert connection.connection_closed.is_set()
        observer.delete(path, recursive=True)


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
    reconnect = subparsers.add_parser("check-zookeeper-reconnect")
    reconnect.add_argument("config_file")
    options = parser.parse_args()
    if options.fault == "corrupt-file":
        corrupt_file(options.path, options.action)
    elif options.fault == "fail-backup-publication":
        fail_backup_publication(options.backup_name, options.arguments)
    elif options.fault == "drop-frozen-table":
        drop_frozen_table(options.database, options.table, options.arguments)
    elif options.fault == "interrupt-inplace-copy":
        interrupt_inplace_copy(options.filename, options.arguments)
    elif options.fault == "check-zookeeper-reconnect":
        check_zookeeper_reconnect(options.config_file)
    else:
        fail_part_attach(options.database, options.table, options.arguments)
