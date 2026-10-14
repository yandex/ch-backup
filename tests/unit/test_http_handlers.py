"""SQL-defined HTTP handler backup and restore tests."""

# pylint: disable=protected-access

from unittest.mock import MagicMock, call

import pytest

from ch_backup.backup.metadata import BackupMetadata
from ch_backup.backup.sources import BackupSources
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


@pytest.mark.parametrize(
    "factory", [BackupSources.for_backup, BackupSources.for_restore]
)
def test_sources_include_handlers_by_default_and_allow_only_handlers(factory):
    assert factory().http_handlers
    assert factory(schema_only=True).http_handlers
    assert not factory(udf=True).http_handlers
    assert factory(http_handlers=True) == BackupSources(
        access=False,
        data=False,
        schema=False,
        udf=False,
        named_collections=False,
        workload_entities=False,
        http_handlers=True,
    )


def test_backup_uploads_sql_and_records_names(manager, context):
    manager.backup(context)
    assert context.backup_meta.get_http_handlers() == ["test"]
    context.backup_layout.upload_http_handler.assert_called_once_with(
        "backup", "test", STATEMENT
    )


def test_backup_skips_old_clickhouse(manager, context):
    context.ch_ctl.ch_version_ge.return_value = False
    manager.backup(context)
    context.ch_ctl.get_http_handlers_create_queries.assert_not_called()
    assert not context.backup_meta.get_http_handlers()


def test_restore_skips_identical_handler(manager, context):
    context.backup_meta.add_http_handler("test")
    manager.restore(context)
    context.ch_ctl.drop_http_handler.assert_not_called()
    context.ch_ctl.restore_http_handler.assert_not_called()


@pytest.mark.parametrize("existing", [{}, {"test": STATEMENT + " + 1"}])
def test_restore_creates_missing_or_replaces_changed_handler(
    manager, context, existing
):
    context.backup_meta.add_http_handler("test")
    context.ch_ctl.get_http_handlers_create_queries.return_value = existing
    manager.restore(context)
    assert context.ch_ctl.drop_http_handler.call_args_list == (
        [call("test")] if existing else []
    )
    context.ch_ctl.restore_http_handler.assert_called_once_with(STATEMENT)


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


def test_restore_reads_all_definitions_before_dropping(manager, context):
    context.backup_meta.add_http_handler("test")
    context.backup_meta.add_http_handler("missing")
    context.backup_layout.get_http_handler_create_statement.side_effect = [
        STATEMENT + " + 1",
        RuntimeError("missing object"),
    ]
    with pytest.raises(RuntimeError, match="missing object"):
        manager.restore(context)
    context.ch_ctl.drop_http_handler.assert_not_called()


def test_restore_empty_legacy_backup_does_not_query_handlers(manager, context):
    manager.restore(context)
    context.ch_ctl.get_http_handlers_create_queries.assert_not_called()


def test_restore_to_old_clickhouse_fails_if_handlers_exist(manager, context):
    context.backup_meta.add_http_handler("test")
    context.ch_ctl.ch_version_ge.return_value = False
    with pytest.raises(ClickhouseBackupError, match="26.8"):
        manager.restore(context)
    context.ch_ctl.restore_http_handler.assert_not_called()


def test_control_exports_sql_with_secrets_enabled(ctl):
    assert ctl.get_http_handlers_create_queries() == {"test": STATEMENT}
    query = ctl._ch_client.query
    assert query.call_args_list[0] == call(
        "CHECK GRANT SHOW HANDLERS ON *.*", settings={"default_format": "JSON"}
    )
    assert query.call_args.kwargs == {
        "settings": {"format_display_secrets_in_show_and_select": 1},
        "sensitive": True,
    }


def test_control_rejects_missing_show_privilege_instead_of_empty_backup(ctl):
    ctl._ch_client.query.side_effect = [{"data": [{"result": 0}]}]
    with pytest.raises(ClickhouseBackupError, match="SHOW HANDLERS"):
        ctl.get_http_handlers_create_queries()


def test_control_rejects_masked_secrets(ctl):
    ctl._ch_client.query.side_effect = [
        {"data": [{"result": 1}]},
        {"data": [{"name": "test", "create_query": STATEMENT + " '[HIDDEN]'"}]},
        {"data": [{"result": 0}]},
    ]
    with pytest.raises(
        ClickhouseBackupError, match="display_secrets_in_show_and_select"
    ):
        ctl.get_http_handlers_create_queries()


@pytest.mark.parametrize("server_setting", ["0", "1"])
def test_control_distinguishes_literal_hidden_marker_from_masked_secret(
    ctl, server_setting
):
    statement = STATEMENT + " WHERE '[HIDDEN]' != ''"
    ctl._ch_client.query.side_effect = [
        {"data": [{"result": 1}]},
        {"data": [{"name": "test", "create_query": statement}]},
        {"data": [{"result": 1}]},
        {"data": [{"value": server_setting}]},
    ]
    if server_setting == "0":
        with pytest.raises(
            ClickhouseBackupError, match="display_secrets_in_show_and_select"
        ):
            ctl.get_http_handlers_create_queries()
    else:
        assert ctl.get_http_handlers_create_queries() == {"test": statement}
        assert ctl._ch_client.query.call_args_list[2] == call(
            "CHECK GRANT displaySecretsInShowAndSelect ON *.*",
            settings={"default_format": "JSON"},
        )


def test_control_restores_sensitive_sql_without_retry(ctl):
    ctl.restore_http_handler(STATEMENT)
    ctl._ch_client.query.assert_called_once_with(
        STATEMENT,
        sensitive=True,
        should_retry=False,
    )


@pytest.mark.parametrize(
    "name, expected",
    [("a b", "`a b`"), ("a`b", "`a\\`b`"), ("a\\b", "`a\\\\b`")],
)
def test_control_quotes_handler_name_when_dropping(ctl, name, expected):
    ctl.drop_http_handler(name)
    ctl._ch_client.query.assert_called_once_with(
        f"DROP HANDLER {expected}",
        sensitive=True,
        should_retry=False,
    )
