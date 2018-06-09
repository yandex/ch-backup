"""
Steps related to ClickHouse and backups.
"""
import yaml
from behave import given, then, when
from hamcrest import assert_that, equal_to, has_length, is_not, matches_regexp
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
@when('clickhouse on {node:w} has test schema')
def step_init_test_schema(context, node):
    """
    Load test schema to clickhouse.
    """
    ClickhouseClient(context, node).init_schema()


@given('{node:w} has test clickhouse data {test_name:w}')
@when('{node:w} has test clickhouse data {test_name:w}')
def step_fill_with_test_data(context, node, test_name):
    """
    Load test_data to clickhouse
    """
    ClickhouseClient(context, node).init_data(mark=test_name)


@given('we have dropped test table #{table_num:d} in db #{db_num:d} on {node}')
@when('we drop test table #{table_num:d} in db #{db_num:d} on {node}')
def step_drop_test_table(context, table_num, db_num, node):
    """
    Drop test table
    """
    ClickhouseClient(context, node).drop_test_table(
        db_num=db_num, table_num=table_num)


@given('we have created {node:w} clickhouse backup')
@when('we create {node:w} clickhouse backup')
def step_create_backup(context, node):
    options = yaml.load(context.text or '') or {}
    backup_id = BackupManager(context, node).backup(**options)
    assert_that(backup_id, matches_regexp('^[0-9]{8}T[0-9]{6}$'))


@then('ch_backup entries of {node:w} are in proper condition')
def step_check_backups_conditions(context, node):
    ch_backup = BackupManager(context, node)
    backup_ids = ch_backup.get_backup_ids()

    # check backup's count
    expected_backups_count = len(context.table.rows)
    current_backups_count = len(backup_ids)
    assert_that(
        current_backups_count, equal_to(expected_backups_count),
        'Backups count = {0}, expected {1}'.format(current_backups_count,
                                                   expected_backups_count))

    # check backup contents
    for row in context.table:
        backup = ch_backup.get_backup(backup_ids[int(row['num'])])

        # check if all backup's files exists
        missed_paths = ch_backup.get_missed_paths(backup.name)
        assert_that(missed_paths, equal_to([]),
                    '{0} missed files were found'.format(len(missed_paths)))

        # check given backup properties
        for cond in context.table.headings:
            if cond in ('num', 'title'):
                continue
            current_value = str(getattr(backup, cond))
            expected_value = row[cond]
            assert_that(
                current_value, equal_to(expected_value),
                'Backup #{0} "{1}": expected {2} = {3}, but was {4}'.format(
                    row['title'], row['num'], cond, expected_value,
                    current_value))


@when('we purge {node} clickhouse backups')
def step_purge_backups(context, node):
    """
    Purge backups
    """
    backup_ids = BackupManager(context, node).purge()
    assert_that(backup_ids, matches_regexp('^([0-9]{8}T[0-9]{6}\n)*$'))


@given('create time of backup #{backup_num:d} of {node:w} was adjusted to'
       ' following delta')
@when('we adjust create time of backup #{backup_num:d} of {node:w}'
      ' to following delta')
def step_adjust_backup_mtime(context, backup_num, node):
    """
    Adjust mtime of specified backup
    """
    ch_backup = BackupManager(context, node)
    assert_that(
        ch_backup.adjust_backup_ctime(backup_num, yaml.load(context.text)),
        is_not(equal_to(None)))


@given('ch-backup config on {node:w} was merged with following')
@when('we merge ch-backup config on {node:w} with following')
def step_update_ch_backup_config(context, node):
    conf = yaml.load(context.text)
    BackupManager(context, node).update_config(conf)


@when('we delete {node:w} clickhouse backup #{backup_num:d}')
def step_delete_backup(context, node, backup_num):
    backup_id = BackupManager(context, node).delete(backup_num)
    assert_that(backup_id, matches_regexp('^([0-9]{8}T[0-9]{6}\n)*$'))


@when('we restore clickhouse #{backup_num:d} backup to {node:w}')
def step_restore_backup(context, backup_num, node):
    backup_id = BackupManager(context, node).restore(backup_num)
    assert_that(backup_id, matches_regexp('^$'))


@when('we restore clickhouse {backup_num:d} backup schema to {node:w}')
def step_restore_backup_schema_only(context, backup_num, node):
    backup_id = BackupManager(context, node).restore(
        backup_num, schema_only=True)
    assert_that(backup_id, matches_regexp('^$'))


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
