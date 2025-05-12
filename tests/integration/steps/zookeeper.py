"""
Steps for interacting with ZooKeeper.
"""

from behave import given, then, when
from hamcrest import assert_that, has_length

from tests.integration.modules.steps import get_step_data
from tests.integration.modules.zookeeper import (
    delete_znode,
    get_children_list,
    initialize_zookeeper_roots,
    write_znode,
    znode_exists,
)


@given("a working zookeeper on {node:w}")
def step_wait_for_zookeeper_alive(context, node):
    initialize_zookeeper_roots(context, node)


@when("on {node:w} we create {znode}")
@then("on {node:w} we create {znode}")
def step_create_znode(context, node, znode):
    write_znode(context, node, znode, b"")


@when("on {node:w} we create {znode} with data")
@then("on {node:w} we create {znode} with data")
def step_create_znode_with_data(context, node, znode):
    write_znode(context, node, znode, context.text.encode())


@when("we execute ZK list query on {node:w}")
def step_zk_list_query(context, node):
    result = get_children_list(context, node, context.text)
    if result is None:
        context.response = []
    else:
        context.response = ";".join(map(str, result))


@then("we get ZK list with len {length:d}")
def step_zk_list_len(context, length):
    response = (
        context.response
        if isinstance(context.response, list)
        else context.response.split(";")
    )
    assert_that(response, has_length(length))


@when("we acquire zookeeper lock on {node:w} with path {zk_lock_path}")
def step_acquire_zookeeper_lock(context, node, zk_lock_path):
    write_znode(context, node, zk_lock_path + "/__lock__-0000000000", b"")


@when("we release zookeeper lock on {node:w} with path {zk_lock_path}")
def step_release_zookeeper_lock(context, node, zk_lock_path):
    delete_znode(context, node, zk_lock_path + "/__lock__-0000000000")


@then("there are no zk node on {node:w}")
def step_zk_node_not_exists(context, node):
    data = get_step_data(context)
    assert not znode_exists(context, node, data["zookeeper_path"])
