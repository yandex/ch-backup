"""
Cli unit tests.
"""

from unittest.mock import MagicMock

from click import Context

from ch_backup import cli
from ch_backup.backup.sources import BackupSources
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
