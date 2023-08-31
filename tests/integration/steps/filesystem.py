"""
Steps related to filesystem.
"""
from behave import then, when
from hamcrest import assert_that, equal_to
from tests.integration.modules.docker import get_container


@when("we create filesystem lock on {node:w} with path {path}")
def step_create_filesystem_lock(context, node, path):
    container = get_container(context, node)
    cmd = f'flock -x {path} --command "sleep 100000"'
    container.exec_run(cmd, detach=True)


@then("we delete filesystem lock on {node:w} with path {path}")
def step_delete_filesystem_lock(context, node, path):
    container = get_container(context, node)
    pgrep_cmd = f"pgrep  -f 'flock.*{path}'"
    pgrep_res = container.exec_run(pgrep_cmd)
    assert_that(
        pgrep_res.exit_code,
        equal_to(0),
        f"Command: {pgrep_cmd} Have non zero exit code. Exit code = {pgrep_res.exit_code}",
    )
    pid = pgrep_res.output.decode()
    assert container.exec_run(f"kill -9 {pid}").exit_code == 0
