"""
Unit tests for the check command.
"""

import json
from unittest.mock import MagicMock

import pytest
from click import Group
from click.testing import CliRunner

from ch_backup import cli
from ch_backup.backup.checker import BackupCheckResult


@pytest.mark.parametrize("format_", ["json", "table"])
def test_check_command(format_: str) -> None:
    backup = MagicMock()
    backup.check.return_value = [
        BackupCheckResult("b1", "created", [], 1, ['"db"."t": unique_key']),
        BackupCheckResult("b2", "created", ["part all_1_1_0: broken"], 0, []),
    ]

    result = CliRunner(mix_stderr=False).invoke(
        Group(commands=[cli.check_command]),
        ["check", "b1", "b2", "--deep", "--format", format_],
        obj={"backup": backup},
    )

    assert result.exit_code == 2
    backup.check.assert_called_once_with(
        ["b1", "b2"], deep=True, workers=4, tmp_dir=None
    )
    if format_ == "json":
        rows = json.loads(result.stdout)
        assert [(r["name"], r["status"], r["unverified"]) for r in rows] == [
            ("b1", "ok", 1),
            ("b2", "broken", 0),
        ]
        assert rows[0]["skipped_data"] == ['"db"."t": unique_key']
        assert rows[1]["problems"] == ["part all_1_1_0: broken"]
    else:
        rows = [line.split() for line in result.stdout.splitlines()[2:]]
        assert result.stdout.splitlines()[0].split()[4:] == [
            "skipped_data",
            "problems",
        ]
        assert [row[:4] for row in rows] == [
            ["b1", "created", "ok", "1"],
            ["b2", "created", "broken", "0"],
        ]
        assert rows[0][4:] == ['"db"."t":', "unique_key"]
        assert rows[1][4:] == ["part", "all_1_1_0:", "broken"]


@pytest.mark.parametrize("args", [[], ["b1", "--all"]])
def test_check_command_requires_backups_or_all(args: list[str]) -> None:
    backup = MagicMock()

    result = CliRunner().invoke(
        Group(commands=[cli.check_command]), ["check", *args], obj={"backup": backup}
    )

    assert result.exit_code == 2
    assert "exactly 1 of the following parameters must be set" in result.output
    backup.check.assert_not_called()


def test_check_command_checks_all_backups() -> None:
    backup = MagicMock()
    backup.check.return_value = []

    result = CliRunner().invoke(
        Group(commands=[cli.check_command]), ["check", "--all"], obj={"backup": backup}
    )

    assert result.exit_code == 0
    backup.check.assert_called_once_with(None, deep=False, workers=4, tmp_dir=None)


@pytest.mark.parametrize(
    "state, exit_code",
    [
        ("created", 2),
        (None, 2),
        ("failed", 0),
        ("creating", 0),
        ("partially_deleted", 0),
    ],
)
def test_check_command_exit_code(state: str | None, exit_code: int) -> None:
    backup = MagicMock()
    backup.check.return_value = [BackupCheckResult("b1", state, ["broken"], 0, [])]

    result = CliRunner().invoke(
        Group(commands=[cli.check_command]), ["check", "--all"], obj={"backup": backup}
    )

    assert result.exit_code == exit_code
