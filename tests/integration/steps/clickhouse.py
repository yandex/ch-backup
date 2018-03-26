"""
Steps related to clickhouse and backups.
"""

import requests
from behave import given, then, when
from hamcrest import (assert_that, equal_to, has_entries, has_length,
                      matches_regexp)
from retrying import retry

from tests.integration.helpers import clickhouse, docker


@given('a working clickhouse on {node_name}')
@retry(wait_fixed=200, stop_max_attempt_number=25)
def step_wait_for_clickhouse_alive(context, node_name):
    """
    Wait until clickhouse is ready to accept incoming requests.
    """

    ch_url = clickhouse.get_base_url(context, node_name)
    requests.packages.urllib3.disable_warnings()  # pylint: disable=no-member
    response = requests.get('{0}/ping'.format(ch_url), verify=False)
    response.raise_for_status()


@given('clickhouse on {node_name} has test schema')
def step_init_test_schema(context, node_name):
    """
    Load test schema to clickhouse.
    """
    ch_client = clickhouse.get_ch_client(context, node_name)
    clickhouse.init_schema(ch_client)


@given('{node_name} has test clickhouse data {test_name}')
def step_fill_with_test_data(context, node_name, test_name):
    """
    Load test_data to clickhouse
    """
    ch_client = clickhouse.get_ch_client(context, node_name)
    clickhouse.fill_with_data(ch_client, mark=test_name)


@when('we create {node_name} clickhouse backup')
def step_create_backup(context, node_name):
    backup_id = clickhouse.make_backup(
        docker.get_container(context, node_name))
    assert_that(backup_id, matches_regexp('^[0-9]{8}T[0-9]{6}$'))


@then('we got {backups_count} ch_backup entries of {node_name}')
def step_check_backup_entries(context, backups_count, node_name):
    ch_instance = docker.get_container(context, node_name)
    ch_client = clickhouse.get_ch_client(context, node_name)
    version = clickhouse.get_version(ch_client)
    backup_entries = clickhouse.get_backup_entries(ch_instance)

    assert_that(backup_entries, has_length(int(backups_count)))
    for entry in backup_entries:
        metadata = clickhouse.get_backup_meta(ch_instance, entry)
        assert_that(metadata,
                    has_entries({
                        'meta': has_entries({
                            'ch_version': version,
                        }),
                    }))


@then('deduplicated {links_count} parts in #{entry_num} ch_backup entry of'
      ' {node_name}')
def step_count_deduplicated_parts(context, links_count, entry_num, node_name):
    dedup_parts_count = clickhouse.count_deduplicated_parts(
        context, node_name, int(entry_num))
    assert_that(dedup_parts_count, equal_to(int(links_count)))


def restore_backup(context, node_name, backup_num, options):
    """
    Restore backup
    """
    ch_instance = docker.get_container(context, node_name)
    response = clickhouse.restore_backup_num(
        ch_instance, backup_num, options=options)
    assert_that(response, equal_to(''))


@when('we restore clickhouse #{backup_num:d} backup to {node_name:w}')
def step_restore_backup(context, backup_num, node_name):
    restore_backup(context, node_name, backup_num, None)


@when('we restore clickhouse {backup_num:d} backup schema to {node_name:w}')
def step_restore_backup_schema_only(context, backup_num, node_name):
    restore_backup(context, node_name, backup_num, options=['--schema-only'])


@then('we got same clickhouse data at {nodes_list}')
def step_same_clickhouse_data(context, nodes_list):
    user_data = []
    for node_name in nodes_list.split():
        ch_client = clickhouse.get_ch_client(context, node_name)
        _, rows_data = clickhouse.get_all_user_data(ch_client)
        user_data.append(rows_data)

    node1_data = user_data[0]
    for node_num in range(1, len(user_data)):
        node_data = user_data[node_num]
        assert_that(node_data, equal_to(node1_data))


@when('we drop all databases at {node_name:w}')
def step_drop_databases(context, node_name):
    ch_client = clickhouse.get_ch_client(context, node_name)
    for db_name in clickhouse.get_all_user_databases(ch_client):
        clickhouse.drop_database(ch_client, db_name)


@then('{node_name1:w} has same schema as {node_name2:w}')
def step_has_same_schema(context, node_name1, node_name2):
    def _get_ddl(node_name):
        ch_client = clickhouse.get_ch_client(context, node_name)
        return clickhouse.get_all_user_schemas(ch_client)

    assert_that(_get_ddl(node_name1), equal_to(_get_ddl(node_name2)))


@then('on {node_name:w} tables are empty')
def step_check_tables_are_empty(context, node_name):
    ch_client = clickhouse.get_ch_client(context, node_name)
    row_count, _ = clickhouse.get_all_user_data(ch_client)
    assert_that(row_count, equal_to(0))
