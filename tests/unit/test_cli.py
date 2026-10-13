"""
Cli unit tests.
"""

import json
from unittest.mock import MagicMock

import pytest
from click import Command, Context, Group
from click.testing import CliRunner, Result

from ch_backup import cli
from ch_backup.backup.metadata import BackupMetadata, BackupState
from ch_backup.backup.sources import BackupSources
from ch_backup.exceptions import StorageError
from tests.unit.utils import parametrize


@parametrize(
    {
        "id": "empty config values",
        "args": {
            "values": [],
            "expected": {},
        },
    },
    {
        "id": "single config values",
        "args": {
            "values": [
                ("backup", {"path_root": "/root"}),
            ],
            "expected": {
                "backup": {
                    "path_root": "/root",
                }
            },
        },
    },
    {
        "id": "plain config values",
        "args": {
            "values": [
                ("backup.path_root", "/root"),
                ("backup.deduplication_age_limit.days", 123),
                ("backup.keep_freezed_data_on_failure", True),
            ],
            "expected": {
                "backup": {
                    "path_root": "/root",
                    "deduplication_age_limit": {
                        "days": 123,
                    },
                    "keep_freezed_data_on_failure": True,
                }
            },
        },
    },
    {
        "id": "complex config values",
        "args": {
            "values": [
                (
                    "backup",
                    {
                        "path_root": None,
                        "deduplication_age_limit": {
                            "days": 7,
                        },
                    },
                ),
                ("backup.path_root", "/root"),
                ("backup.deduplication_age_limit", {"days": 8}),
                ("backup.keep_freezed_data_on_failure", True),
            ],
            "expected": {
                "backup": {
                    "path_root": "/root",
                    "deduplication_age_limit": {
                        "days": 8,
                    },
                    "keep_freezed_data_on_failure": True,
                }
            },
        },
    },
    {
        "id": "overrode config values",
        "args": {
            "values": [
                ("backup", "not_used_setting"),
                ("backup", "actual_setting"),
            ],
            "expected": {"backup": "actual_setting"},
        },
    },
    {
        "id": "not uniform config values",
        "args": {
            "values": [
                (
                    "backup",
                    {
                        "path_root": "/root",
                        "deduplication_age_limit": {
                            "days": 7,
                        },
                    },
                ),
                ("backup", "actual_setting"),
            ],
            "expected": {
                "backup": "actual_setting",
            },
        },
    },
)
def test_build_cli_cfg_from_config_parameters(values, expected):
    # pylint: disable=protected-access
    assert cli._build_cli_cfg_from_config_parameters(values) == expected


def _invoke_backup_command(args: list) -> MagicMock:
    """
    Run backup_command with a mocked ClickhouseBackup and return that mock.

    Its config is a mock as well, pylint infers the dict it replaces.
    """
    backup_mock = MagicMock()
    backup_mock.backup.return_value = ("20181017T210300", None)

    parent_ctx = Context(cli.cli)
    parent_ctx.obj = {"backup": backup_mock}
    with cli.backup_command.make_context("backup", args, parent=parent_ctx) as ctx:
        cli.backup_command.invoke(ctx)

    return backup_mock


def test_backup_command_copy_cloud_storage_data_merges_config():
    """
    The flag must be passed to the logic through the configuration.
    """
    backup_mock = _invoke_backup_command(["--copy-cloud-storage-data"])

    backup_mock.config.merge.assert_called_once_with(  # pylint: disable=no-member
        {"cloud_storage": {"copy_data": True}}
    )


def test_backup_command_keep_going_merges_config():
    """
    The flag must be passed to the logic through the configuration, so that a
    service can turn it on without changing how the command is called.
    """
    backup_mock = _invoke_backup_command(["--keep-going"])

    backup_mock.config.merge.assert_called_once_with(  # pylint: disable=no-member
        {"backup": {"keep_going": True}}
    )


def test_backup_command_without_copy_cloud_storage_data_keeps_config():
    """
    Without the flag the configuration must be left as it is.
    """
    backup_mock = _invoke_backup_command([])

    backup_mock.config.merge.assert_not_called()  # pylint: disable=no-member


def test_backup_command_copy_cloud_storage_data_does_not_reload_config():
    """
    reload_config() deletes the lazily created backup context and fails with
    AttributeError when the context has never been accessed.
    """
    backup_mock = _invoke_backup_command(["--copy-cloud-storage-data"])

    backup_mock.reload_config.assert_not_called()


def test_backup_command_copy_cloud_storage_data_is_not_a_backup_source():
    """
    The flag selects how data is stored, not what is backed up, so it must not
    turn the run into a partial backup.
    """
    backup_mock = _invoke_backup_command(["--copy-cloud-storage-data"])

    sources = backup_mock.backup.call_args.args[0]
    assert sources == BackupSources.for_backup(
        False, False, False, False, False, False, False
    )


@pytest.mark.parametrize(
    "endpoint",
    ["https://copied.example:443", "http://copied.example:9000", "copied.example:8443"],
)
def test_restore_command_passes_source_endpoint(endpoint: str) -> None:
    backup = MagicMock()
    backup.list.return_value = [MagicMock()]
    backup.list.return_value[0].name = "backup1"
    parent = Context(cli.cli)
    parent.obj = {"backup": backup}
    with cli.restore_command.make_context(
        "restore",
        ["backup1", "--cloud-storage-source-endpoint", endpoint],
        parent=parent,
    ) as ctx:
        cli.restore_command.invoke(ctx)

    backup.restore.assert_called_once()
    assert backup.restore.call_args.kwargs["cloud_storage_source_endpoint"] == endpoint


def _invoke(command: Command, args: list[str], backup: MagicMock) -> Result:
    """Invoke a CLI command with a mocked ClickhouseBackup."""
    return CliRunner(mix_stderr=False).invoke(
        Group(commands=[command]), args, obj={"backup": backup}
    )


def _backup_list_with_unreadable() -> MagicMock:
    """Mock ClickhouseBackup whose list reports "broken" as unreadable or raises if not asked to."""
    backup = MagicMock()
    good = BackupMetadata("good", "2", "26.6", "%Y-%m-%d")
    good.state = BackupState.CREATED

    def list_backups(_state=None, *, unreadable=None):
        """Return the readable backup and record "broken" as unreadable."""
        if unreadable is None:
            raise StorageError("Unreadable light")
        unreadable["broken"] = "S3RetryingError: InternalError"
        return [good]

    backup.list.side_effect = list_backups
    return backup


@pytest.mark.parametrize("format_", ["json", "table"])
@pytest.mark.parametrize(
    "flags", [[], ["-a"], ["--include-unreadable"], ["-a", "--include-unreadable"]]
)
def test_list_command_with_unreadable_metadata(format_, flags):
    """Unreadable backups are skipped by default and shown as unknown with -a."""
    backup = _backup_list_with_unreadable()
    args = ["list", "-v", "--format", format_, *flags]
    result = _invoke(cli.list_command, args, backup)
    if not flags:
        assert result.exit_code == 0
        assert "good" in result.stdout
        assert "broken" not in result.stdout
        return
    include_unreadable = "--include-unreadable" in flags
    assert result.exit_code == (2 if include_unreadable else 0)
    assert ("Unreadable metadata: 1" in result.stderr) == include_unreadable
    if format_ == "json":
        rows = json.loads(result.stdout)
        assert [row["name"] for row in rows] == ["good", "broken"]
        assert rows[0]["metadata_error"] is None
        assert rows[1]["state"] == "unknown"
        assert rows[1]["metadata_error"] == "S3RetryingError: InternalError"
        assert all(
            value is None
            for key, value in rows[1].items()
            if key not in ("name", "state", "metadata_error")
        )
    else:
        assert all(
            value in result.stdout
            for value in ("good", "broken", "unknown", "InternalError")
        )


def test_list_command_requires_verbose_for_unreadable_entries():
    """Reject the flag combination before reading storage."""
    backup = MagicMock()
    result = CliRunner().invoke(
        Group(commands=[cli.list_command]),
        ["list", "--include-unreadable"],
        obj={"backup": backup},
    )
    assert result.exit_code == 2
    assert "requires --verbose" in result.output
    backup.list.assert_not_called()


def test_restore_command_ignores_unreadable_other_backups():
    """Unreadable metadata of another backup does not block restore."""
    backup = _backup_list_with_unreadable()
    result = _invoke(cli.restore_command, ["restore", "good"], backup)
    assert result.exit_code == 0, result.output
    assert backup.restore.call_args.kwargs["backup_name"] == "good"


def test_last_is_not_resolved_with_unreadable_backups():
    """LAST could silently point to an older backup than the unreadable one."""
    backup = _backup_list_with_unreadable()
    result = _invoke(cli.restore_command, ["restore", "LAST"], backup)
    assert result.exit_code == 2
    assert "Cannot resolve LAST" in result.stderr
    backup.restore.assert_not_called()


def test_delete_command_accepts_unreadable_backup():
    """The backup with unreadable metadata itself can be passed to delete."""
    backup = _backup_list_with_unreadable()
    backup.delete.return_value = ("broken", None)
    result = _invoke(cli.delete_command, ["delete", "broken"], backup)
    assert result.exit_code == 0, result.output
    backup.delete.assert_called_once_with("broken", False)
