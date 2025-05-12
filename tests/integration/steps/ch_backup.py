"""
Steps related to ch-backup command-line tool.
"""

import json

from behave import given, then, when
from hamcrest import (
    any_of,
    assert_that,
    contains_string,
    equal_to,
    has_entries,
    matches_regexp,
    starts_with,
)

from tests.integration.modules.ch_backup import BackupManager
from tests.integration.modules.docker import get_container
from tests.integration.modules.steps import get_step_data


@given("ch-backup configuration on {node:w}")
def step_update_ch_backup_config(context, node):
    conf = get_step_data(context)
    BackupManager(context, node).update_config(conf)


@given("we have created {node:w} clickhouse backup")
@when("we create {node:w} clickhouse backup")
def step_create_backup(context, node):
    options = get_step_data(context)
    name = BackupManager(context, node).backup(**options)
    name_regexp = options.get("name", "{uuid}")
    name_regexp = name_regexp.replace("{timestamp}", "[0-9]{8}T[0-9]{6}")
    name_regexp = name_regexp.replace("{uuid}", "[0-9a-f-]{36}")
    assert_that(name, matches_regexp(name_regexp))


@when("we try to create {node:w} clickhouse backup")
def step_try_create_backup(context, node):
    options = get_step_data(context)
    try:
        BackupManager(context, node).backup(**options)
    except Exception:
        pass


@when("we can't create {node:w} clickhouse backup")
def step_cannot_create_backup(context, node):
    options = get_step_data(context)
    name = BackupManager(context, node).backup(**options)
    assert_that(
        len(name),
        equal_to(0),
        f"On {node} created backup with name : {name}, but we assume that the backup can't be created.",
    )


@given("metadata of {node:w} backup #{backup_id:d} was adjusted with")
@when("metadata of {node:w} backup #{backup_id:d} was adjusted with")
@given('metadata of {node:w} backup "{backup_id}" was adjusted with')
@when('metadata of {node:w} backup "{backup_id}" was adjusted with')
def step_update_backup_metadata(context, node, backup_id):
    ch_backup = BackupManager(context, node)
    context.backup = ch_backup.get_backup(backup_id)
    metadata = get_step_data(context)
    ch_backup.update_backup_metadata(backup_id, metadata)


@when("metadata paths of {node:w} backup #{backup_id:d} was deleted")
def step_backup_metadata_delete(context, node, backup_id):
    paths = get_step_data(context)
    BackupManager(context, node).delete_backup_metadata_paths(backup_id, paths)


@given('file "{path}" was deleted from {node:w} backup #{backup_id:d}')
@given('file "{path}" was deleted from {node:w} backup "{backup_id:d}"')
def step_delete_backup_file(context, path, node, backup_id):
    ch_backup = BackupManager(context, node)
    ch_backup.delete_backup_file(backup_id, path)


@given('file "{path}" in {node:w} backup #{backup_id:d} is empty')
@given('file "{path}" in {node:w} backup "{backup_id:d}" is empty')
def step_truncate_backup_file(context, path, node, backup_id):
    ch_backup = BackupManager(context, node)
    ch_backup.set_backup_file_data(backup_id, path, data=b"")


@given('file "{path}" in {node:w} backup #{backup_id:d} data set to')
@given('file "{path}" in {node:w} backup "{backup_id:d}" data set to')
def step_set_backup_file(context, path, node, backup_id):
    ch_backup = BackupManager(context, node)
    ch_backup.set_backup_file_data(backup_id, path, data=context.text.strip().encode())


@when("we restore clickhouse backup #{backup_id:d} to {node:w}")
@when('we restore clickhouse backup "{backup_id}" to {node:w}')
def step_restore_backup(context, backup_id, node):
    options = get_step_data(context)
    result = BackupManager(context, node).restore(backup_id, **options)
    assert_that(result, matches_regexp("^$"))


@when("we delete {node:w} clickhouse backup #{backup_id:d}")
@when('we delete {node:w} clickhouse backup "{backup_id}"')
def step_delete_backup(context, node, backup_id):
    options = get_step_data(context)
    result = BackupManager(context, node).delete(backup_id, **options)
    assert_that(
        result,
        any_of(
            matches_regexp("^(([0-9]{8}T[0-9]{6}|[0-9a-f-]{36})\\s*)*$"),
            contains_string("Backup was not deleted"),
            contains_string("Backup was partially deleted"),
        ),
    )


@when("we purge {node} clickhouse backups")
def step_purge_backups(context, node):
    BackupManager(context, node).purge()


@then("metadata of {node:w} backup #{backup_id:d} contains")
@then('metadata of {node:w} backup "{backup_id}" contains')
def step_backup_metadata(context, node, backup_id):
    expected_meta = get_step_data(context)

    backup = BackupManager(context, node).get_backup(backup_id)
    assert_that(backup.meta, has_entries(expected_meta))


@then(
    'metadata of {node:w} backup #{backup_id:d} contains value for "{field:w}" which begins with'
)
def step_backup_metadata_value(context, node, backup_id, field):
    expected_value = context.text

    backup = BackupManager(context, node).get_backup(backup_id)
    assert_that(backup.meta[field], starts_with(expected_value))


@then("metadata of {node:w} backup #{backup_id:d} contains no")
@then('metadata of {node:w} backup "{backup_id}" contains no')
def step_backup_metadata_absent(context, node, backup_id):
    expected_meta = get_step_data(context)

    backup = BackupManager(context, node).get_backup(backup_id)
    assert_that(backup.meta, not has_entries(expected_meta))


@then("we got no backups on {node:w}")
def step_no_backups(context, node):
    ch_backup = BackupManager(context, node)
    backup_ids = ch_backup.get_backup_ids()

    backup_count = len(backup_ids)

    assert_that(
        backup_count,
        equal_to(0),
        f"On {node} we got {backup_count} backups, when we expected that there are no backups",
    )


@then("we got the following backups on {node:w}")
def step_check_backups_conditions(context, node):
    ch_backup = BackupManager(context, node)
    backup_ids = ch_backup.get_backup_ids()

    backup_count = len(backup_ids)
    expected_backup_count = len(context.table.rows)
    assert_that(
        backup_count,
        equal_to(expected_backup_count),
        f"Backup count = {backup_count}, expected {expected_backup_count}",
    )
    for i, backup_id in enumerate(backup_ids):
        backup = ch_backup.get_backup(backup_id)
        expected_backup = context.table[i]

        if backup.state in ("created", "partially_deleted"):
            missed_paths = ch_backup.get_missed_paths(backup.name)
            assert_that(
                missed_paths,
                equal_to([]),
                f"{len(missed_paths)} missed files were found",
            )

        for attr in context.table.headings:
            if attr in ("num", "title"):
                continue
            current_value = str(getattr(backup, attr))
            expected_value = expected_backup[attr]
            assert_that(
                current_value,
                equal_to(expected_value),
                f"Backup #{i}: {backup.name} expected {attr} ="
                f" {expected_value}, but was {current_value}",
            )


@when(
    "we restore clickhouse access control metadata backup #{backup_id:d} to {node:w} with restart"
)
@when(
    'we restore clickhouse access control metadata backup "{backup_id}" to {node:w} with restart'
)
def step_restore_access_control_backup(context, backup_id, node):
    result = BackupManager(context, node).restore_access_control(backup_id)
    assert_that(result, matches_regexp("^$"))
    container = get_container(context, node)
    assert container.exec_run("supervisorctl restart clickhouse").exit_code == 0


@when("we restart clickhouse on {node:w}")
def step_restart_clickhouse(context, node):
    container = get_container(context, node)
    assert container.exec_run("supervisorctl restart clickhouse").exit_code == 0


@then("we got the following s3 backup directories on {node:w}")
def step_check_s3_backup_directory(context, node):
    backups_directory = "/var/lib/clickhouse/disks/s3/shadow/"
    container = get_container(context, node)
    run_result = container.exec_run(f"ls {backups_directory}", user="root")
    run_output = run_result.output.decode().strip()
    actual_directories = list(run_output.split())

    expected_directories = json.loads(context.text)

    assert_that(
        actual_directories,
        equal_to(expected_directories),
        f"Actual backup directories = {actual_directories}, expected {expected_directories}",
    )


@then("we got a valid ch-backup version on {node:w}")
def step_get_ch_backup_version(context, node):
    assert_that(
        BackupManager(context, node).version(),
        equal_to(context.version),
        f"Ch-backup version is {BackupManager(context, node).version()}, expected {context.version}",
    )
