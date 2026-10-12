"""
Cli unit tests.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click import Context, Group
from click.testing import CliRunner

from ch_backup import cli
from ch_backup.backup.copy_report import BackupCopyResult
from ch_backup.backup.sources import BackupSources
from ch_backup.exceptions import TerminatingSignal
from tests.unit.utils import parametrize


@pytest.mark.parametrize(
    "failure", [None, RuntimeError("copy failed"), TerminatingSignal("interrupted")]
)
def test_copy_command_writes_report_on_success_error_and_signal(
    tmp_path: Path, failure: BaseException | None
) -> None:
    backup = MagicMock()
    report_path = tmp_path / "report.json"

    def copy(_target, _names, *, keep_going, report):
        assert keep_going
        report.backups.append(
            BackupCopyResult(
                "b1",
                status="copied",
                objects_total=1,
                objects_copied=1,
                known_bytes_copied=42,
            )
        )
        if failure:
            report.backups.append(BackupCopyResult("b2", status="failed"))
            report.backups.append(BackupCopyResult("b3"))
            raise failure
        return ["b1"], []

    backup.copy.side_effect = copy
    group = Group()
    group.add_command(cli.copy_command)
    runner = CliRunner()
    args = ["copy", "--all", "--keep-going", "--report-path", str(report_path)]
    if isinstance(failure, TerminatingSignal):
        with pytest.raises(TerminatingSignal):
            runner.invoke(group, args, obj={"backup": backup})
    else:
        result = runner.invoke(group, args, obj={"backup": backup})
        assert result.exit_code == (1 if failure else 0)
        assert "b1: copied" in result.output
        assert "Copy summary:" in result.output
        if failure:
            assert result.exception is failure
            assert "b3: not_processed" in result.output
    report = json.loads(report_path.read_text())
    assert report["schema_version"] == 1
    assert report["error"] == (
        f"{type(failure).__name__}: {failure}" if failure else None
    )
    assert report["backups"][0]["known_bytes_copied"] == 42
    assert "secret_access_key" not in report_path.read_text()


@pytest.mark.parametrize("copy_failed", [False, True])
def test_copy_report_write_failure_preserves_original_exception(
    tmp_path: Path, copy_failed: bool
) -> None:
    backup = MagicMock()
    failure = RuntimeError("primary error")
    if copy_failed:
        backup.copy.side_effect = failure
    group = Group()
    group.add_command(cli.copy_command)
    result = CliRunner().invoke(
        group,
        ["copy", "--all", "--report-path", str(tmp_path / "missing" / "report.json")],
        obj={"backup": backup},
    )
    assert result.exit_code == 1
    assert "Failed to save copy report" in result.output
    if copy_failed:
        assert result.exception is failure


def test_copy_reports_configuration_failure(tmp_path: Path) -> None:
    backup = MagicMock()
    # pylint: disable-next=no-member
    backup.config.derive.side_effect = ValueError("invalid config")
    group = Group()
    group.add_command(cli.copy_command)
    path = tmp_path / "report.json"
    result = CliRunner().invoke(
        group, ["copy", "--all", "--report-path", str(path)], obj={"backup": backup}
    )
    assert result.exit_code == 1
    assert json.loads(path.read_text())["error"] == "ValueError: invalid config"
    backup.copy.assert_not_called()


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
