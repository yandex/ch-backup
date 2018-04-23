"""
Steps related to ClickHouse and backups.
"""

from behave import given, then, when
from hamcrest import assert_that, equal_to, has_length, matches_regexp
from retrying import retry

from tests.integration.helpers.ch_backup import BackupManager
from tests.integration.helpers.ch_client import ClickhouseClient


@given('a working clickhouse on {node_name:w}')
@retry(wait_fixed=200, stop_max_attempt_number=25)
def step_wait_for_clickhouse_alive(context, node_name):
    """
    Wait until clickhouse is ready to accept incoming requests.
    """
    ClickhouseClient(context, node_name).ping()


@given('clickhouse on {node_name:w} has test schema')
def step_init_test_schema(context, node_name):
    """
    Load test schema to clickhouse.
    """
    ClickhouseClient(context, node_name).init_schema()


@given('{node_name:w} has test clickhouse data {test_name:w}')
def step_fill_with_test_data(context, node_name, test_name):
    """
    Load test_data to clickhouse
    """
    ClickhouseClient(context, node_name).init_data(mark=test_name)


@when('we create {node_name:w} clickhouse backup')
def step_create_backup(context, node_name):
    backup_id = BackupManager(context, node_name).backup()
    assert_that(backup_id, matches_regexp('^[0-9]{8}T[0-9]{6}$'))


@then('we got {backup_count:d} ch_backup entries of {node_name:w}')
def step_check_backup_entries(context, backup_count, node_name):
    version = ClickhouseClient(context, node_name).get_version()
    ch_backup = BackupManager(context, node_name)
    backup_ids = ch_backup.get_backup_ids()

    assert_that(backup_ids, has_length(backup_count))
    for backup_id in backup_ids:
        backup = ch_backup.get_backup(backup_id)
        assert_that(backup.version, equal_to(version))


@then('deduplicated {link_count:d} parts in #{entry_num:d} ch_backup entry of'
      ' {node_name:w}')
def step_count_deduplicated_parts(context, link_count, entry_num, node_name):
    backup = BackupManager(context, node_name).get_backup(entry_num)
    assert_that(backup.link_count, equal_to(link_count))


@when('we restore clickhouse #{backup_num:d} backup to {node_name:w}')
def step_restore_backup(context, backup_num, node_name):
    BackupManager(context, node_name).restore(backup_num)


@when('we restore clickhouse {backup_num:d} backup schema to {node_name:w}')
def step_restore_backup_schema_only(context, backup_num, node_name):
    BackupManager(context, node_name).restore(backup_num, schema_only=True)


@then('we got same clickhouse data at {nodes_list}')
def step_same_clickhouse_data(context, nodes_list):
    user_data = []
    for node_name in nodes_list.split():
        ch_client = ClickhouseClient(context, node_name)
        _, rows_data = ch_client.get_all_user_data()
        user_data.append(rows_data)

    node1_data = user_data[0]
    for node_num in range(1, len(user_data)):
        node_data = user_data[node_num]
        assert_that(node_data, equal_to(node1_data))


@when('we drop all databases at {node_name:w}')
def step_drop_databases(context, node_name):
    ch_client = ClickhouseClient(context, node_name)
    for db_name in ch_client.get_all_user_databases():
        ch_client.drop_database(db_name)


@then('{node_name1:w} has same schema as {node_name2:w}')
def step_has_same_schema(context, node_name1, node_name2):
    def _get_ddl(node_name):
        ch_client = ClickhouseClient(context, node_name)
        return ch_client.get_all_user_schemas()

    assert_that(_get_ddl(node_name1), equal_to(_get_ddl(node_name2)))


@then('on {node_name:w} tables are empty')
def step_check_tables_are_empty(context, node_name):
    ch_client = ClickhouseClient(context, node_name)
    row_count, _ = ch_client.get_all_user_data()
    assert_that(row_count, equal_to(0))
