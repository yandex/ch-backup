"""Faults that must run inside the ClickHouse container."""

import argparse
from typing import cast
from unittest.mock import patch

from click import Command

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import BackupState
from ch_backup.cli import cli


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

    # The patch must live in the CLI process; workers=0 keeps uploads there too.
    with patch.object(BackupLayout, "upload_backup_metadata", fail_owner):
        cast(Command, cli).main(
            ["--config-parameter", "multiprocessing.workers", "0", *arguments]
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="fault", required=True)
    corruption = subparsers.add_parser("corrupt-file")
    corruption.add_argument("path")
    corruption.add_argument("action", choices=["flip_byte", "truncate"])
    publication = subparsers.add_parser("fail-backup-publication")
    publication.add_argument("backup_name")
    publication.add_argument("arguments", nargs=argparse.REMAINDER)
    options = parser.parse_args()
    if options.fault == "corrupt-file":
        corrupt_file(options.path, options.action)
    else:
        fail_backup_publication(options.backup_name, options.arguments)
