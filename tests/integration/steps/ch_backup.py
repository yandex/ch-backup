"""
Steps related to ch-backup command-line tool.
"""
from behave import given, then, when
from hamcrest import (any_of, assert_that, contains_string, equal_to, has_entries, matches_regexp)

from tests.integration.modules.ch_backup import BackupManager
from tests.integration.modules.steps import get_step_data


@given('ch-backup config on {node:w} was merged with following')
def step_update_ch_backup_config(context, node):
    conf = get_step_data(context)
    BackupManager(context, node).update_config(conf)


@given('we have created {node:w} clickhouse backup')
@when('we create {node:w} clickhouse backup')
def step_create_backup(context, node):
    options = get_step_data(context)

    name = BackupManager(context, node).backup(**options)

    name_regexp = options.get('name', '{timestamp}')
    name_regexp = name_regexp.replace('{timestamp}', '[0-9]{8}T[0-9]{6}')
    name_regexp = name_regexp.replace('{uuid}', '[0-9a-f-]{36}')
    assert_that(name, matches_regexp(name_regexp))


@given('metadata of {node:w} backup #{backup_id:d} was adjusted with')
@given('metadata of {node:w} backup "{backup_id}" was adjusted with')
def step_update_backup_metadata(context, node, backup_id):
    ch_backup = BackupManager(context, node)
    context.backup = ch_backup.get_backup(backup_id)
    metadata = get_step_data(context)
    ch_backup.update_backup_metadata(backup_id, metadata)


@when('we restore clickhouse backup #{backup_id:d} to {node:w}')
@when('we restore clickhouse backup "{backup_id}" to {node:w}')
def step_restore_backup(context, backup_id, node):
    options = get_step_data(context)
    result = BackupManager(context, node).restore(backup_id, **options)
    assert_that(result, matches_regexp('^$'))


@when('we delete {node:w} clickhouse backup #{backup_id:d}')
@when('we delete {node:w} clickhouse backup "{backup_id}"')
def step_delete_backup(context, node, backup_id):
    result = BackupManager(context, node).delete(backup_id)
    assert_that(
        result,
        any_of(matches_regexp('^([0-9]{8}T[0-9]{6}\\s*)*$'), contains_string('Backup was not deleted'),
               contains_string('Backup was partially deleted')))


@when('we purge {node} clickhouse backups')
def step_purge_backups(context, node):
    BackupManager(context, node).purge()


@then('metadata of {node:w} backup #{backup_id:d} contains')
@then('metadata of {node:w} backup "{backup_id}" contains')
def step_backup_metadata(context, node, backup_id):
    expected_meta = get_step_data(context)

    backup = BackupManager(context, node).get_backup(backup_id)
    assert_that(backup.meta, has_entries(expected_meta))


@then('we got the following backups on {node:w}')
def step_check_backups_conditions(context, node):
    ch_backup = BackupManager(context, node)
    backup_ids = ch_backup.get_backup_ids()

    backup_count = len(backup_ids)
    expected_backup_count = len(context.table.rows)
    assert_that(backup_count, equal_to(expected_backup_count),
                f'Backup count = {backup_count}, expected {expected_backup_count}')

    for i, backup_id in enumerate(backup_ids):
        backup = ch_backup.get_backup(backup_id)
        expected_backup = context.table[i]

        # check that all backup's files exist
        missed_paths = ch_backup.get_missed_paths(backup.name)
        assert_that(missed_paths, equal_to([]), '{0} missed files were found'.format(len(missed_paths)))

        # check backup properties
        for attr in context.table.headings:
            if attr in ('num', 'title'):
                continue
            current_value = str(getattr(backup, attr))
            expected_value = expected_backup[attr]
            assert_that(current_value, equal_to(expected_value), f'Backup #{i}: {backup.name} expected {attr} ='
                        f' {expected_value}, but was {current_value}')
