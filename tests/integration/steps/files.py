"""Reusable faults for ClickHouse part files and their copies."""

from uuid import uuid4

from behave import given, then, when

from tests.integration.modules.clickhouse import ClickhouseClient, get_part_path
from tests.integration.modules.docker import get_container
from tests.integration.modules.faults import FAULT_MODULE, run_fault
from tests.integration.modules.steps import get_step_data


@then('part "{part}" of {database:w}.{table:w} is not published on {node:w}')
def step_part_not_published(context, part, database, table, node):
    paths = (
        ClickhouseClient(context, node)
        .get_response(
            f"SELECT arrayJoin(data_paths) FROM system.tables "
            f"WHERE database = '{database}' AND name = '{table}' FORMAT TSVRaw"
        )
        .splitlines()
    )
    assert paths, "Expected a restored table"
    container = get_container(context, node)
    for path in paths:
        for predicate in ("-e", "-L"):
            result = container.exec_run(
                ["test", "!", predicate, f"{path}/detached/{part}"]
            )
            assert (
                result.exit_code == 0
            ), f"Part was published at {path}/detached/{part}"


@when(
    "we execute ch-backup on {node:w} with frozen table {database:w}.{table:w} dropped before metadata upload"
)
def step_backup_dropped_frozen_table(context, node, database, table):
    run_fault(context, node, "drop-frozen-table", database, table)
    assert context.exit_code == 0, context.response


@when(
    'we try to execute ch-backup on {node:w} with inplace copy interrupted after "{filename}"'
)
def step_interrupt_inplace_copy(context, node, filename):
    run_fault(context, node, "interrupt-inplace-copy", filename)


@when(
    "we try to execute ch-backup on {node:w} with ATTACH failing for {database:w}.{table:w}"
)
def step_fail_part_attach(context, node, database, table):
    run_fault(context, node, "fail-part-attach", database, table)


@given("we save and corrupt a data part file on {node:w}")
@when("we save and corrupt a data part file on {node:w}")
def step_corrupt_part_file(context, node):
    conf = get_step_data(context)
    path = get_part_path(context, node, conf["database"], conf["table"], conf["part"])
    path = f"{path}/{conf['file']}"
    saved = getattr(context, "saved_part_files", {})
    assert (node, path) not in saved, f"File already saved: {path}"
    original = f"/tmp/part-file-{uuid4()}"
    container = get_container(context, node)
    result = container.exec_run(["cp", "--", path, original], user="root")
    assert result.exit_code == 0, result.output
    saved[node, path] = original
    context.saved_part_files = saved
    result = container.exec_run(
        ["python3", "-m", FAULT_MODULE, "corrupt-file", path, conf["action"]],
        user="root",
        workdir="/code",
    )
    assert result.exit_code == 0, result.output


@given("we restore saved data part files")
@when("we restore saved data part files")
def step_restore_part_files(context):
    saved = getattr(context, "saved_part_files", {})
    assert saved, "No saved part files to restore"
    for (node, path), original in saved.items():
        container = get_container(context, node)
        # cp writes through the existing inode, preserving ClickHouse ownership
        # and the hard links retained by FREEZE, just like the original fixture.
        result = container.exec_run(["cp", "--", original, path], user="root")
        assert result.exit_code == 0, result.output
        result = container.exec_run(["cmp", "--", original, path], user="root")
        assert result.exit_code == 0, result.output
        result = container.exec_run(["rm", "--", original], user="root")
        assert result.exit_code == 0, result.output
    context.saved_part_files = {}


@given("data part files on {node:w} are unreadable by ClickHouse")
@when("data part files on {node:w} are unreadable by ClickHouse")
def step_unreadable_part_files(context, node):
    conf = get_step_data(context)
    path = get_part_path(context, node, conf["database"], conf["table"], conf["part"])
    container = get_container(context, node)
    result = container.exec_run(
        ["find", path, "-maxdepth", "1", "-type", "f", "-name", conf["pattern"]],
        user="root",
    )
    assert result.exit_code == 0, result.output
    paths = result.output.decode().splitlines()
    assert paths, f"No files matching {conf['pattern']} in {path}"
    for filename in paths:
        result = container.exec_run(["chmod", "000", filename], user="root")
        assert result.exit_code == 0, result.output
        result = container.exec_run(["cat", filename], user="clickhouse")
        assert result.exit_code != 0, f"ClickHouse can still read {filename}"
