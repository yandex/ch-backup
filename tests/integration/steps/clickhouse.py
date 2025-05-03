"""
Steps for interacting with ClickHouse DBMS.
"""

import yaml
from behave import given, then, when
from hamcrest import assert_that, equal_to, has_length
from tenacity import retry, stop_after_attempt, wait_fixed

from tests.integration.modules.ch_backup import BackupManager
from tests.integration.modules.clickhouse import ClickhouseClient
from tests.integration.modules.docker import get_container, put_file
from tests.integration.modules.steps import get_step_data
from tests.integration.modules.templates import render_template


@given("a working clickhouse on {node:w}")
@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(360))
def step_wait_for_clickhouse_alive(context, node):
    """
    Wait until clickhouse is ready to accept incoming requests.
    """
    ClickhouseClient(context, node).ping()


@given("we have enabled shared zookeeper for {node:w}")
def step_enable_shared_zookeeper_for_clickhouse(context, node):
    """
    Replace a part of CH config on the fly to enable shared zookeeper for clickhouse nodes.
    """
    container = get_container(context, node)

    override_config = "/config/shared_zookeeper.xml"
    assert (
        container.exec_run(
            f"ln -s {override_config} /etc/clickhouse-server/conf.d/"
        ).exit_code
        == 0
    )
    assert container.exec_run("supervisorctl restart clickhouse").exit_code == 0

    shared_zk_path = "/" + render_template(context, "{{ conf.zk.shared_node }}")

    dict_to_update = {"zookeeper": {"root_path": shared_zk_path}}
    BackupManager(context, node).update_config(dict_to_update)


@given("clickhouse on {node:w} has test schema")
@when("clickhouse on {node:w} has test schema")
def step_init_test_schema(context, node):
    """
    Load test schema to clickhouse.
    """
    ClickhouseClient(context, node).init_schema()


@given(
    "clickhouse on {node:w} has test schema with {db_count:d} databases and {tables_count:d} tables"
)
@when(
    "clickhouse on {node:w} has test schema with {db_count:d} databases and {tables_count:d} tables"
)
def step_init_test_schema_custom(context, node, db_count, tables_count):
    """
    Load test schema to clickhouse.
    """
    ClickhouseClient(context, node).init_schema(db_count, tables_count)


@when('we put following info in "{path}" at {node:w}')
def step_put_file(context, path, node):
    container = get_container(context, node)
    put_file(container, bytes(context.text, "utf-8"), path)


@given("{node:w} has test clickhouse data {test_name:w}")
@when("{node:w} has test clickhouse data {test_name:w}")
def step_fill_with_test_data(context, node, test_name):
    """
    Load test data to clickhouse.
    """
    ClickhouseClient(context, node).init_data(mark=test_name)


@given(
    "{node:w} has test clickhouse data {test_name:w} with {db_count:d} databases, {tables_count:d} tables, {rows_count:d} rows and {partitions_count:d} partitions"
)
@when(
    "{node:w} has test clickhouse data {test_name:w} with {db_count:d} databases, {tables_count:d} tables, {rows_count:d} rows and {partitions_count:d} partitions"
)
# pylint: disable=too-many-positional-arguments
def step_fill_with_test_data_custom(
    context,
    node,
    test_name,
    db_count,
    tables_count,
    rows_count,
    partitions_count,
):
    """
    Load test data to clickhouse.
    """
    ClickhouseClient(context, node).init_data(
        mark=test_name,
        db_count=db_count,
        table_count=tables_count,
        rows_count=rows_count,
        partitions_count=partitions_count,
    )


@given("we execute query on {node:w}")
@when("we execute query on {node:w}")
def step_test_request(context, node):
    ch_client = ClickhouseClient(context, node)
    context.response = ch_client.get_response(render_template(context, context.text))


@given("we have executed queries on {node:w}")
@when("we execute queries on {node:w}")
def step_test_data(context, node):
    queries = []
    for string in context.text.split(";"):
        string = string.strip()
        if string:
            queries.append(string)
    ch_client = ClickhouseClient(context, node)
    for query in queries:
        ch_client.execute(query)


@given("we have dropped test table #{table_num:d} in db #{db_num:d} on {node}")
@when("we drop test table #{table_num:d} in db #{db_num:d} on {node}")
def step_drop_test_table(context, table_num, db_num, node):
    ClickhouseClient(context, node).drop_test_table(db_num=db_num, table_num=table_num)


@then("we get response")
def step_get_response(context):
    assert_that(context.response, equal_to(context.text))


@then("we got same clickhouse data at {nodes}")
def step_same_clickhouse_data(context, nodes):
    options = get_step_data(context)
    user_data = []
    for node in nodes.split():
        ch_client = ClickhouseClient(context, node, **options)
        _, rows_data = ch_client.get_all_user_data()
        user_data.append(rows_data)
    node1_data = user_data[0]
    for node_num in range(1, len(user_data)):
        node_data = user_data[node_num]
        assert_that(node_data, equal_to(node1_data))


@then("{node1:w} has the subset of {node2:w} data")
def step_has_subset_data(context, node1, node2):
    options = yaml.load(context.text, yaml.SafeLoader)
    tables = options["tables"]
    node_data = {}
    for node in (node1, node2):
        ch_client = ClickhouseClient(context, node)
        _, node_data[node] = ch_client.get_all_user_data()
    assert_that(node_data[node1], has_length(len(tables)))
    for table in tables:
        assert_that(node_data[node1][table], equal_to(node_data[node2][table]))


@when("we drop all databases at {node:w}")
def step_drop_databases(context, node):
    ch_client = ClickhouseClient(context, node)
    for db_name in ch_client.get_all_user_databases():
        ch_client.drop_database(db_name)


@when("we drop all data at {node:w}")
def step_drop_accesses(context, node):
    ch_client = ClickhouseClient(context, node)
    # drop databases
    for db_name in ch_client.get_all_user_databases():
        ch_client.drop_database(db_name)
    # drop all access entities
    ch_client.drop_all_access_objects()
    # drop all udf
    ch_client.drop_all_udf()


@then("{node1:w} has same schema as {node2:w}")
def step_has_same_schema(context, node1, node2):
    def _get_ddl(node):
        ch_client = ClickhouseClient(context, node)
        return ch_client.get_table_schemas()

    assert_that(_get_ddl(node1), equal_to(_get_ddl(node2)))


@then("{node1:w} has same access control objects as {node2:w}")
def step_has_same_access(context, node1, node2):
    def _get_ddl(node):
        ch_client = ClickhouseClient(context, node)
        return ch_client.get_all_access_objects()

    assert_that(_get_ddl(node1), equal_to(_get_ddl(node2)))


@then("{node1:w} has same named collections as {node2:w}")
def step_has_same_named_collections(context, node1, node2):
    def _get_ddl(node):
        ch_client = ClickhouseClient(context, node)
        return ch_client.get_all_named_collections()

    assert_that(_get_ddl(node1), equal_to(_get_ddl(node2)))


@then("on {node:w} tables are empty")
def step_check_tables_are_empty(context, node):
    ch_client = ClickhouseClient(context, node)
    row_count, _ = ch_client.get_all_user_data()
    assert_that(row_count, equal_to(0))


@given("dirty removed clickhouse data at {node:w}")
@when("we dirty remove clickhouse data at {node:w}")
def step_dirty_remove_data(context, node):
    container = get_container(context, node)
    assert container.exec_run("supervisorctl stop clickhouse").exit_code == 0
    assert container.exec_run("rm -rf /var/lib/clickhouse/data").exit_code == 0
    assert container.exec_run("rm -rf /var/lib/clickhouse/metadata").exit_code == 0
    assert container.exec_run("rm -rf /var/lib/clickhouse/store").exit_code == 0
    assert container.exec_run("rm -rf /var/lib/clickhouse/disks").exit_code == 0
    assert container.exec_run("rm -rf /var/lib/clickhouse/access").exit_code == 0
    assert container.exec_run("supervisorctl start clickhouse").exit_code == 0


@when("we drop restore context at {node:w}")
def step_drop_restore_context(context, node):
    container = get_container(context, node)
    assert container.exec_run("rm -rf /tmp/ch_backup_restore_state.json").exit_code == 0


@given("we have dirty enabled replicated access on {node:w} with restart")
@when("we dirty enable replicated access on {node:w} with restart")
def step_dirty_enable_replicated_access(context, node):
    """
    Replace a part of CH config on the fly to enable replicated access storage.
    """
    container = get_container(context, node)

    override_config = "/config/user_directories_replicated.xml"
    assert (
        container.exec_run(
            f"ln -s {override_config} /etc/clickhouse-server/conf.d/"
        ).exit_code
        == 0
    )
    assert container.exec_run("supervisorctl restart clickhouse").exit_code == 0
    assert container.exec_run("rm -rf /var/lib/clickhouse/access").exit_code == 0


@given(
    "we replace config file {config_to_replace} in favor of {new_config} on {node:w} with restart"
)
@when(
    "we replace config file {config_to_replace} in favor of {new_config} on {node:w} with restart"
)
def step_replace_config_file(context, config_to_replace, new_config, node):
    """
    Replace a part of CH config on the fly.
    """
    container = get_container(context, node)

    assert (
        container.exec_run(
            f"rm -rf /etc/clickhouse-server/conf.d/{config_to_replace}"
        ).exit_code
        == 0
    )
    assert (
        container.exec_run(
            f"ln -s /config/{new_config} /etc/clickhouse-server/conf.d/"
        ).exit_code
        == 0
    )
    assert container.exec_run("supervisorctl restart clickhouse").exit_code == 0


@when("we stop clickhouse at {node:w}")
def step_stop_clickhouse(context, node):
    container = get_container(context, node)
    result = container.exec_run(
        ["bash", "-c", "supervisorctl stop clickhouse"], user="root"
    )
    context.response = result.output.decode().strip()
    context.exit_code = result.exit_code


@when("we start clickhouse at {node:w}")
def step_start_clickhouse(context, node):
    container = get_container(context, node)
    result = container.exec_run(
        ["bash", "-c", "supervisorctl start clickhouse"], user="root"
    )
    context.response = result.output.decode().strip()
    context.exit_code = result.exit_code


@when("we save all user's data in context on {node:w}")
def step_save_user_data(context, node):
    ch_client = ClickhouseClient(context, node)
    context.user_data = ch_client.get_all_user_data()


@then("the user's data equal to saved one on {node:w}")
def step_check_data_equal(context, node):
    ch_client = ClickhouseClient(context, node)
    new_user_data = ch_client.get_all_user_data()
    assert new_user_data == context.user_data


@then("database replica {database} on {node:w} does not exists")
def step_check_no_database_replica(context, database, node):
    ch_client = ClickhouseClient(context, node)
    assert not ch_client.is_database_replica_exists(database)
