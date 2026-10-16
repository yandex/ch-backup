"""SQL-defined HTTP handler backup and restore tests."""

# pylint: disable=protected-access

from unittest.mock import MagicMock, call

import pytest

from ch_backup.backup.metadata import BackupMetadata
from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.exceptions import ClickhouseBackupError
from ch_backup.logic.http_handlers import HTTPHandlersBackup

STATEMENT = "CREATE HANDLER `test` URL '/test' AS SELECT 1"


@pytest.fixture(name="metadata")
def metadata_fixture():
    return BackupMetadata("backup", "2", "26.8", "%Y-%m-%d")


@pytest.fixture(name="context")
def context_fixture(metadata):
    ctx = MagicMock()
    ctx.backup_meta = metadata
    ctx.ch_ctl.ch_version_ge.return_value = True
    ctx.ch_ctl.get_http_handlers_create_queries.return_value = {"test": STATEMENT}
    ctx.backup_layout.get_http_handler_create_statement.return_value = STATEMENT
    return ctx


@pytest.fixture(name="manager")
def manager_fixture():
    return HTTPHandlersBackup()


@pytest.fixture(name="ctl")
def ctl_fixture():
    control = object.__new__(ClickhouseCTL)
    control._ch_client = MagicMock()
    control._ch_client.query.side_effect = [
        {"data": [{"result": 1}]},
        {"data": [{"name": "test", "create_query": STATEMENT}]},
    ]
    return control


def test_metadata_roundtrip_and_legacy_default(metadata):
    metadata.add_http_handler("test")
    assert BackupMetadata.load_json(metadata.dump_json()).get_http_handlers() == [
        "test"
    ]
    assert metadata.dump(light=True)["http_handlers"] == []
    data = metadata.dump()
    del data["http_handlers"]
    assert BackupMetadata.load(data).get_http_handlers() == []


def test_restore_drops_changed_handlers_before_creating_to_allow_url_swaps(
    manager, context
):
    first = "CREATE HANDLER a URL '/b' AS SELECT 1"
    second = "CREATE HANDLER b URL '/a' AS SELECT 1"
    context.backup_meta.add_http_handler("a")
    context.backup_meta.add_http_handler("b")
    context.ch_ctl.get_http_handlers_create_queries.return_value = {
        "a": "CREATE HANDLER a URL '/a' AS SELECT 1",
        "b": "CREATE HANDLER b URL '/b' AS SELECT 1",
    }
    context.backup_layout.get_http_handler_create_statement.side_effect = [
        first,
        second,
    ]
    manager.restore(context)
    assert context.ch_ctl.method_calls == [
        call.ch_version_ge("26.8"),
        call.get_http_handlers_create_queries(),
        call.drop_http_handler("a"),
        call.drop_http_handler("b"),
        call.restore_http_handler(first),
        call.restore_http_handler(second),
    ]


def test_restore_to_old_clickhouse_fails_if_handlers_exist(manager, context):
    context.backup_meta.add_http_handler("test")
    context.ch_ctl.ch_version_ge.return_value = False
    with pytest.raises(ClickhouseBackupError, match="26.8"):
        manager.restore(context)
    context.ch_ctl.restore_http_handler.assert_not_called()


def test_control_rejects_missing_show_privilege_instead_of_empty_backup(ctl):
    ctl._ch_client.query.side_effect = [{"data": [{"result": 0}]}]
    with pytest.raises(ClickhouseBackupError, match="SHOW HANDLERS"):
        ctl.get_http_handlers_create_queries()


def test_control_rejects_hidden_marker_even_if_later_checks_allow_secrets(ctl):
    statement = (
        "CREATE HANDLER test URL '/test' AS SELECT * FROM "
        "s3('https://example.invalid/data.csv', 'key', '[HIDDEN]', 'CSV', 'x UInt64')"
    )
    ctl._ch_client.query.side_effect = [
        {"data": [{"result": 1}]},
        {"data": [{"name": "test", "create_query": statement}]},
        {"data": [{"result": 1}]},
        {"data": [{"value": "1"}]},
    ]
    with pytest.raises(ClickhouseBackupError, match="hidden secrets"):
        ctl.get_http_handlers_create_queries()
    # Later permission/configuration values cannot establish visibility at read time.
    assert ctl._ch_client.query.call_count == 2


def test_control_restores_sensitive_sql_without_retry(ctl):
    ctl.restore_http_handler(STATEMENT)
    ctl._ch_client.query.assert_called_once_with(
        STATEMENT,
        sensitive=True,
        should_retry=False,
    )


@pytest.mark.parametrize(
    "name, expected",
    [("a`b", "`a\\`b`"), ("a\\b", "`a\\\\b`")],
)
def test_control_quotes_handler_name_when_dropping(ctl, name, expected):
    ctl.drop_http_handler(name)
    ctl._ch_client.query.assert_called_once_with(
        f"DROP HANDLER {expected}",
        sensitive=True,
        should_retry=False,
    )
