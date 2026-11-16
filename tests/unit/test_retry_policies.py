"""Command-level S3 retry policy tests."""

import copy
from unittest.mock import MagicMock, patch

import pytest
from click import Group
from click.testing import CliRunner

from ch_backup import cli
from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.command_context import command_context, get_current_command
from ch_backup.config import DEFAULT_CONFIG
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.s3.s3_retry import (
    resolve_s3_retry_config,
)


def test_restore_cli_uses_one_policy_for_validation_and_restore() -> None:
    backup = MagicMock()
    metadata = MagicMock()
    metadata.name = "backup"
    seen: list[tuple[str, str | None]] = []

    def list_backups(*_args, **_kwargs):
        seen.append(("list", get_current_command()))
        return [metadata]

    def restore_backup(*_args, **_kwargs):
        seen.append(("restore", get_current_command()))

    backup.list.side_effect = list_backups
    backup.restore.side_effect = restore_backup

    result = CliRunner().invoke(
        Group(commands=[cli.restore_command]),
        ["restore", "backup"],
        obj={"backup": backup},
    )

    assert result.exit_code == 0, result.output
    assert seen == [
        ("list", "restore"),
        ("restore", "restore"),
    ]


def test_s3_retry_policy_is_selected_by_command() -> None:
    expected_by_command = {
        "list": (5, 10),
        "show": (5, 10),
        "copy": (60, 60),
        "restore": (60, 60),
        "restore-from-replica": (60, 60),
        "version": (10, 60),
    }
    for command, expected in expected_by_command.items():
        with command_context(command):
            assert resolve_s3_retry_config(DEFAULT_CONFIG["storage"]) == expected


def test_all_retry_policies_are_validated_before_s3_client_creation() -> None:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["storage"]["retry"]["restore"]["max_attempts"] = 0

    with (
        patch("ch_backup.storage.engine.s3.s3_engine.S3ClientFactory") as factory,
        pytest.raises(ConfigurationError, match=r"storage\.retry\.restore"),
    ):
        ClickhouseBackup(config)  # type: ignore[arg-type]

    factory.assert_not_called()
