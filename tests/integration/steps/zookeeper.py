"""
Steps for interacting with ZooKeeper.
"""
from behave import given, then, when

from tests.integration.modules.zookeeper import (initialize_zookeeper_roots, write_znode)


@given('a working zookeeper on {node:w}')
def step_wait_for_zookeeper_alive(context, node):
    initialize_zookeeper_roots(context, node)


@when('on {node:w} we create {znode}')
@then('on {node:w} we create {znode}')
def step_create_znode(context, node, znode):
    write_znode(context, node, znode, b'')


@when('on {node:w} we create {znode} with data')
@then('on {node:w} we create {znode} with data')
def step_create_znode_with_data(context, node, znode):
    write_znode(context, node, znode, context.text.encode())
