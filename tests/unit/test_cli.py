"""
Cli unit tests.
"""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click import BadParameter, Command, Context, Group
from click.testing import CliRunner, Result

from ch_backup import cli
from ch_backup.backup.copy_report import BackupCopyResult
from ch_backup.backup.metadata import BackupMetadata, BackupState, TableMetadata
from ch_backup.backup.metadata.table_metadata import DataSkippedReason
from ch_backup.backup.sources import BackupSources
from ch_backup.clickhouse.models import Database
from ch_backup.config import DEFAULT_CONFIG, Config
from ch_backup.exceptions import ConfigurationError, StorageError, TerminatingSignal
from tests.unit.test_backup_copy import CopyTestContext, _backup, _part
from tests.unit.utils import parametrize


@pytest.fixture(name="copy_backup")
def copy_backup_fixture(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("")
    return MagicMock(config=Config(str(path)))


@pytest.mark.parametrize(
    "failure", [None, RuntimeError("copy failed"), TerminatingSignal("interrupted")]
)
def test_copy_command_writes_report_on_success_error_and_signal(
    tmp_path: Path, failure: BaseException | None, copy_backup: MagicMock
) -> None:
    backup = copy_backup
    report_path = tmp_path / "report.json"

    def copy(_target, _names, *, keep_going, report, command_start):
        assert keep_going
        assert command_start.tzinfo is not None
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
    tmp_path: Path, copy_failed: bool, copy_backup: MagicMock
) -> None:
    backup = copy_backup
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


@pytest.mark.parametrize(
    "args,resume", [([], True), (["--resume"], True), (["--no-resume"], False)]
)
def test_copy_command_passes_resume(
    args: list[str], resume: bool, copy_backup: MagicMock
) -> None:
    backup = copy_backup
    group = Group()
    group.add_command(cli.copy_command)

    result = CliRunner().invoke(group, ["copy", "--all", *args], obj={"backup": backup})

    assert result.exit_code == 0
    target_config = backup.copy.call_args.args[0]
    assert target_config["copy"]["resume"] is resume


@pytest.mark.parametrize(
    "args,expected", [([], "full"), (["--no-only-with-data"], "schema")]
)
def test_copy_last_skips_empty_backups(
    args: list[str], expected: str, copy_backup: MagicMock
) -> None:
    """LAST is resolved to the latest backup with data parts unless told otherwise."""
    schema_backup = _backup("schema")
    schema_backup.schema_only = True
    copy_backup.list.return_value = [
        schema_backup,
        _backup("no-parts"),
        _backup("full", parts={"all_1_1_0": _part()}),
    ]

    result = CliRunner().invoke(
        Group(commands=[cli.copy_command]),
        ["copy", "LAST", *args],
        obj={"backup": copy_backup},
    )

    assert result.exit_code == 0, result.output
    assert copy_backup.copy.call_args.args[1] == [expected]


def test_copy_last_fails_with_only_empty_backups(copy_backup: MagicMock) -> None:
    """LAST is not resolved when no backup has data parts."""
    schema_backup = _backup("schema")
    schema_backup.schema_only = True
    copy_backup.list.return_value = [schema_backup, _backup("no-parts")]

    result = CliRunner().invoke(
        Group(commands=[cli.copy_command]),
        ["copy", "LAST"],
        obj={"backup": copy_backup},
    )

    assert result.exit_code == 2
    assert "There are no backups with data." in result.output
    copy_backup.copy.assert_not_called()


@pytest.mark.parametrize("state", [None, BackupState.FAILED])
@pytest.mark.parametrize("keep_going", [False, True])
def test_copy_named_validation_failure_has_report_row(
    tmp_path: Path, state: BackupState | None, keep_going: bool, copy_backup: MagicMock
) -> None:
    ctx = CopyTestContext([_backup("requested", state=state)] if state else [])
    backup = copy_backup

    def copy(_target, names, *, keep_going, report, command_start):
        assert command_start.tzinfo is not None
        return ctx.copy(names, keep_going=keep_going, report=report)

    backup.copy.side_effect = copy
    group = Group()
    group.add_command(cli.copy_command)
    path = tmp_path / "report.json"
    args = ["copy", "requested", "--report-path", str(path)]
    if keep_going:
        args.append("--keep-going")
    result = CliRunner().invoke(group, args, obj={"backup": backup})
    assert result.exit_code == 1
    report = json.loads(path.read_text())
    assert [(b["name"], b["status"]) for b in report["backups"]] == [
        ("requested", "failed"),
    ]
    assert report["backups"][0]["errors"][0]["stage"] == "selection"
    backup.list.assert_not_called()


@pytest.mark.parametrize("days", [-1, 90])
def test_copy_window_cli_override_before_last_lookup(copy_backup, days, monkeypatch):
    command_start = datetime(2026, 10, 22, tzinfo=timezone.utc)
    clock = [command_start]
    monkeypatch.setattr(cli, "utcnow", lambda: clock[0])

    def list_backups(*_args, **_kwargs):
        clock[0] += timedelta(seconds=30)
        return [_backup("selected", parts={"all_1_1_0": _part()})]

    copy_backup.config["copy"]["deduplication_age_limit"] = {"days": 7}
    copy_backup.list.side_effect = list_backups
    result = CliRunner().invoke(
        Group(commands=[cli.copy_command]),
        [
            "copy",
            "LAST",
            "--target-config-parameter",
            "copy.deduplication_age_limit.days",
            str(days),
        ],
        obj={"backup": copy_backup},
    )
    if days < 0:
        assert isinstance(result.exception, ConfigurationError)
        copy_backup.list.assert_not_called()
        copy_backup.copy.assert_not_called()
    else:
        assert result.exit_code == 0, result.output
        config, names = copy_backup.copy.call_args.args
        assert names == ["selected"]
        assert copy_backup.copy.call_args.kwargs["command_start"] == command_start
        assert config["copy"]["deduplication_age_limit"] == {"days": days}
        assert copy_backup.config["copy"]["deduplication_age_limit"] == {"days": 7}


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


def _invoke_backup_command(args: list, backup_config: dict | None = None) -> MagicMock:
    """
    Run backup_command with a mocked ClickhouseBackup and return that mock.

    Its config is a mock as well, pylint infers the dict it replaces. Indexing it
    returns the default config with backup_config merged into the backup section.
    """
    config = deepcopy(DEFAULT_CONFIG)
    config["backup"].update(backup_config or {})  # type: ignore[union-attr]
    backup_mock = MagicMock()
    backup_mock.config.__getitem__.side_effect = config.__getitem__
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


@pytest.mark.parametrize(
    ("option", "expected"),
    [([], False), (["--restore-detached-parts"], True)],
)
def test_restore_command_passes_detached_parts_option(
    option: list[str], expected: bool
) -> None:
    backup = MagicMock()
    backup.list.return_value = [MagicMock()]
    backup.list.return_value[0].name = "backup1"
    parent = Context(cli.cli)
    parent.obj = {"backup": backup}
    with cli.restore_command.make_context(
        "restore", ["backup1", *option], parent=parent
    ) as ctx:
        cli.restore_command.invoke(ctx)

    backup.restore.assert_called_once()
    assert backup.restore.call_args.kwargs["restore_detached_parts"] is expected


@pytest.mark.parametrize(
    ("option", "kwarg"),
    [("--databases", "databases"), ("--exclude-databases", "exclude_databases")],
)
def test_restore_command_accepts_quoted_database_names(option: str, kwarg: str) -> None:
    """
    ClickHouse allows any characters in a quoted database name, so the CLI must
    pass names with a hyphen through as they are.
    """
    backup = MagicMock()
    backup.list.return_value = [MagicMock()]
    backup.list.return_value[0].name = "backup1"
    parent = Context(cli.cli)
    parent.obj = {"backup": backup}
    with cli.restore_command.make_context(
        "restore", ["backup1", option, "db-one, db_two"], parent=parent
    ) as ctx:
        cli.restore_command.invoke(ctx)

    assert backup.restore.call_args.kwargs[kwarg] == ["db-one", "db_two"]


def test_backup_command_accepts_quoted_database_names() -> None:
    """
    Database names with a hyphen must reach the backup logic as they are.
    """
    backup_mock = _invoke_backup_command(["--databases", "db-one,db_two"])

    assert backup_mock.backup.call_args.kwargs["db_names"] == ["db-one", "db_two"]


def test_restore_command_rejects_empty_database_name() -> None:
    """
    An empty item in the list is a typo and must fail instead of being passed on.
    """
    parent = Context(cli.cli)
    parent.obj = {"backup": MagicMock()}
    with pytest.raises(BadParameter):
        cli.restore_command.make_context(
            "restore", ["backup1", "--exclude-databases", "db1,,db2"], parent=parent
        )


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


@pytest.mark.parametrize("format_", ["json", "table"])
def test_list_command_reports_tables_with_skipped_data(format_):
    """An incomplete backup is told apart without reading its full metadata."""
    backup = MagicMock()
    incomplete = BackupMetadata("incomplete", "2", "26.6", "%Y-%m-%d")
    incomplete.state = BackupState.CREATED
    incomplete.add_database(Database("db", "Atomic", None, None, None))
    incomplete.add_table(
        TableMetadata(
            "db", "skipped", "MergeTree", None, DataSkippedReason.BACKUP_FAILED
        )
    )
    incomplete.add_table(TableMetadata("db", "partial", "MergeTree", None))
    incomplete.skip_part("db", "partial", "all_1_1_0")
    backup.list.return_value = [incomplete]

    result = _invoke(cli.list_command, ["list", "-v", "--format", format_], backup)

    assert result.exit_code == 0
    if format_ == "json":
        row = json.loads(result.stdout)[0]
        assert (row["data_skipped_tables"], row["incomplete_tables"]) == (1, 1)
    else:
        assert "data_skipped_tables" in result.stdout
        assert "incomplete_tables" in result.stdout


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


def test_backup_help_lists_detached_parts_filters() -> None:
    result = CliRunner().invoke(cli.cli.commands["backup"], ["--help"])

    assert result.exit_code == 0
    assert "--detached-parts" in result.output
    assert "--detached-parts-disks" in result.output
    assert "--detached-parts-reasons" in result.output


def test_empty_detached_reason_is_preserved_by_cli_parser() -> None:
    with cli.cli.commands["backup"].make_context(
        "backup", ["--detached-parts-reason", ""]
    ) as context:
        assert context.params["detached_parts_reasons"] == [""]


def test_detached_disks_are_parsed_as_a_list() -> None:
    with cli.cli.commands["backup"].make_context(
        "backup",
        ["--detached-parts-disk", "object_storage,archive_storage"],
    ) as context:
        assert context.params["detached_parts_disks"] == [
            "object_storage",
            "archive_storage",
        ]


def test_detached_parts_are_opt_in_with_object_storage_default() -> None:
    backup_mock = _invoke_backup_command([])

    assert backup_mock.backup.call_args.kwargs["include_detached_parts"] is False
    assert backup_mock.backup.call_args.kwargs["detached_parts_disks"] == [
        "object_storage"
    ]


def test_detached_filter_without_opt_in_does_not_enable_backup() -> None:
    backup_mock = _invoke_backup_command(["--detached-parts-disk", "default"])

    assert backup_mock.backup.call_args.kwargs["include_detached_parts"] is False
    assert backup_mock.backup.call_args.kwargs["detached_parts_disks"] == ["default"]


@parametrize(
    {
        "id": "config sources without options",
        "args": {
            "options": [],
            "expected": BackupSources.for_backup(schema=True),
        },
    },
    {
        "id": "options replace config sources",
        "args": {
            "options": ["--access"],
            "expected": BackupSources.for_backup(access=True),
        },
    },
    {
        "id": "http handlers replace config sources",
        "args": {
            "options": ["--http-handlers"],
            "expected": BackupSources.for_backup(http_handlers=True),
        },
    },
    {
        "id": "schema-only replaces config sources",
        "args": {
            "options": ["--schema-only"],
            "expected": BackupSources.for_backup(schema_only=True),
        },
    },
)
def test_backup_sources_from_config(options: list, expected: BackupSources) -> None:
    backup_mock = _invoke_backup_command(options, {"sources": {"schema": True}})

    assert backup_mock.backup.call_args.args[0] == expected


def test_detached_parts_from_config() -> None:
    backup_mock = _invoke_backup_command(
        [],
        {"detached_parts": {"enabled": True, "disks": ["default"], "reasons": [""]}},
    )

    kwargs = backup_mock.backup.call_args.kwargs
    assert kwargs["include_detached_parts"] is True
    assert kwargs["detached_parts_disks"] == ["default"]
    assert kwargs["detached_parts_reasons"] == [""]


def test_detached_parts_options_override_config() -> None:
    backup_mock = _invoke_backup_command(
        ["--detached-parts-disk", "archive", "--detached-parts-reason", "broken"],
        {"detached_parts": {"enabled": True, "disks": ["default"], "reasons": [""]}},
    )

    kwargs = backup_mock.backup.call_args.kwargs
    assert kwargs["include_detached_parts"] is True
    assert kwargs["detached_parts_disks"] == ["archive"]
    assert kwargs["detached_parts_reasons"] == ["broken"]


def test_detached_parts_enabled_must_be_boolean() -> None:
    with pytest.raises(ConfigurationError):
        _invoke_backup_command([], {"detached_parts": {"enabled": "false"}})


def test_config_parameter_null_clears_config_sources(tmp_path: Path) -> None:
    config_path = tmp_path / "ch-backup.conf"
    config_path.write_text("backup: {sources: {schema: true}}")
    config = Config(str(config_path))

    config.merge({"backup": {"sources": None}})

    assert BackupSources.from_config(config["backup"]["sources"]) == BackupSources(
        http_handlers=True
    )


def test_http_handlers_from_config() -> None:
    backup_mock = _invoke_backup_command([], {"sources": {"http_handlers": True}})

    assert backup_mock.backup.call_args.args[0] == BackupSources.for_backup(
        http_handlers=True
    )


@pytest.mark.parametrize("configured", [False, True])
@pytest.mark.parametrize("override", [None, False, True])
def test_copy_only_with_data_config_and_cli(copy_backup, configured, override):
    """CLI overrides the configured filter for both LAST and --all."""
    copy_backup.config["copy"]["only_with_data"] = configured
    copy_backup.list.return_value = [
        _backup("empty"),
        _backup("data", parts={"all_1_1_0": _part()}),
    ]
    args = (
        []
        if override is None
        else ["--only-with-data" if override else "--no-only-with-data"]
    )
    expected = configured if override is None else override
    for selection in ["LAST", "--all"]:
        result = _invoke(cli.copy_command, ["copy", selection, *args], copy_backup)
        assert result.exit_code == 0, result.output
        config, names = copy_backup.copy.call_args.args
        assert config["copy"]["only_with_data"] is expected
        assert names == (
            None if selection == "--all" else ["data" if expected else "empty"]
        )


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, [], {}])
@pytest.mark.parametrize("selection", ["LAST", "--all"])
def test_copy_rejects_non_boolean_only_with_data(copy_backup, value, selection):
    """Reject invalid filter settings before listing or copying backups."""
    copy_backup.config["copy"]["only_with_data"] = value
    result = _invoke(cli.copy_command, ["copy", selection], copy_backup)
    assert isinstance(result.exception, ConfigurationError)
    assert str(result.exception) == "copy.only_with_data must be a boolean"
    copy_backup.list.assert_not_called()
    copy_backup.copy.assert_not_called()
