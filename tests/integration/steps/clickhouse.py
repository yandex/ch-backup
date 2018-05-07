"""
Steps related to ClickHouse and backups.
"""
import yaml
from behave import given, then, when
from hamcrest import assert_that, equal_to, has_length, matches_regexp
from retrying import retry

from tests.integration.helpers.ch_backup import BackupManager
from tests.integration.helpers.ch_client import ClickhouseClient


@given('a working clickhouse on {node:w}')
@retry(wait_fixed=200, stop_max_attempt_number=25)
def step_wait_for_clickhouse_alive(context, node):
    """
    Wait until clickhouse is ready to accept incoming requests.
    """
    ClickhouseClient(context, node).ping()


@given('clickhouse on {node:w} has test schema')
def step_init_test_schema(context, node):
    """
    Load test schema to clickhouse.
    """
    ClickhouseClient(context, node).init_schema()


@given('{node:w} has test clickhouse data {test_name:w}')
def step_fill_with_test_data(context, node, test_name):
    """
    Load test_data to clickhouse
    """
    ClickhouseClient(context, node).init_data(mark=test_name)


@when('we create {node:w} clickhouse backup')
def step_create_backup(context, node):
    options = yaml.load(context.text or '') or {}
    backup_id = BackupManager(context, node).backup(**options)
    assert_that(backup_id, matches_regexp('^[0-9]{8}T[0-9]{6}$'))


@then('we got {backup_count:d} ch_backup entries of {node:w}')
def step_check_backup_entries(context, backup_count, node):
    version = ClickhouseClient(context, node).get_version()
    ch_backup = BackupManager(context, node)
    backup_ids = ch_backup.get_backup_ids()

    assert_that(backup_ids, has_length(backup_count))
    for backup_id in backup_ids:
        backup = ch_backup.get_backup(backup_id)
        assert_that(backup.version, equal_to(version))


@then('deduplicated {link_count:d} parts in #{entry_num:d} ch_backup entry of'
      ' {node:w}')
def step_count_deduplicated_parts(context, link_count, entry_num, node):
    backup = BackupManager(context, node).get_backup(entry_num)
    assert_that(backup.link_count, equal_to(link_count))


@when('we restore clickhouse #{backup_num:d} backup to {node:w}')
def step_restore_backup(context, backup_num, node):
    BackupManager(context, node).restore(backup_num)


@when('we restore clickhouse {backup_num:d} backup schema to {node:w}')
def step_restore_backup_schema_only(context, backup_num, node):
    BackupManager(context, node).restore(backup_num, schema_only=True)


@then('we got same clickhouse data at {nodes}')
def step_same_clickhouse_data(context, nodes):
    user_data = []
    for node in nodes.split():
        ch_client = ClickhouseClient(context, node)
        _, rows_data = ch_client.get_all_user_data()
        user_data.append(rows_data)

    node1_data = user_data[0]
    for node_num in range(1, len(user_data)):
        node_data = user_data[node_num]
        assert_that(node_data, equal_to(node1_data))


@then('{node1:w} has the subset of {node2:w} data')
def step_has_subset_data(context, node1, node2):
    options = yaml.load(context.text)
    tables = options['tables']

    node_data = {}
    for node in (node1, node2):
        ch_client = ClickhouseClient(context, node)
        _, node_data[node] = ch_client.get_all_user_data()

    assert_that(node_data[node1], has_length(len(tables)))
    for table in tables:
        assert_that(node_data[node1][table], equal_to(node_data[node2][table]))


@when('we drop all databases at {node:w}')
def step_drop_databases(context, node):
    ch_client = ClickhouseClient(context, node)
    for db_name in ch_client.get_all_user_databases():
        ch_client.drop_database(db_name)


@then('{node1:w} has same schema as {node2:w}')
def step_has_same_schema(context, node1, node2):
    def _get_ddl(node):
        ch_client = ClickhouseClient(context, node)
        return ch_client.get_all_user_schemas()

    assert_that(_get_ddl(node1), equal_to(_get_ddl(node2)))


@then('on {node:w} tables are empty')
def step_check_tables_are_empty(context, node):
    ch_client = ClickhouseClient(context, node)
    row_count, _ = ch_client.get_all_user_data()
    assert_that(row_count, equal_to(0))
