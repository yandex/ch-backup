"""
Steps related to ch-backup command-line tool.
"""
import yaml
from behave import given, then, when
from hamcrest import (any_of, assert_that, contains_string, equal_to,
                      has_entries, matches_regexp)

from tests.integration.modules.ch_backup import BackupManager
from tests.integration.modules.templates import render_template


@given('ch-backup config on {node:w} was merged with following')
def step_update_ch_backup_config(context, node):
    conf = yaml.load(context.text)
    BackupManager(context, node).update_config(conf)


@given('we have created {node:w} clickhouse backup')
@when('we create {node:w} clickhouse backup')
def step_create_backup(context, node):
    options = yaml.load(context.text or '') or {}
    backup_id = BackupManager(context, node).backup(**options)
    assert_that(backup_id, matches_regexp('[0-9]{8}T[0-9]{6}$'))


@given('metadata of {node:w} backup #{backup_num:d} was adjusted with')
def step_update_backup_metadata(context, node, backup_num):
    ch_backup = BackupManager(context, node)
    context.backup = ch_backup.get_backup(backup_num)
    metadata = yaml.load(render_template(context, context.text))
    ch_backup.update_backup_metadata(backup_num, metadata)


@when('we restore clickhouse #{backup_num:d} backup to {node:w}')
def step_restore_backup(context, backup_num, node):
    backup_id = BackupManager(context, node).restore(backup_num)
    assert_that(backup_id, matches_regexp('^$'))


@when('we restore clickhouse {backup_num:d} backup schema to {node:w}')
def step_restore_backup_schema_only(context, backup_num, node):
    backup_id = BackupManager(context, node).restore(
        backup_num, schema_only=True)
    assert_that(backup_id, matches_regexp('^$'))


@when('we delete {node:w} clickhouse backup #{backup_num:d}')
def step_delete_backup(context, node, backup_num):
    backup_id = BackupManager(context, node).delete(backup_num)
    assert_that(
        backup_id,
        any_of(
            matches_regexp('^([0-9]{8}T[0-9]{6}\n)*$'),
            contains_string('Backup was not deleted'),
            contains_string('Backup was partially deleted')))


@when('we purge {node} clickhouse backups')
def step_purge_backups(context, node):
    backup_ids = BackupManager(context, node).purge()
    assert_that(backup_ids, matches_regexp('^([0-9]{8}T[0-9]{6}\n)*$'))


@then('{node:w} backup #{backup_num:d} metadata contains')
def step_backup_metadata(context, node, backup_num):
    expected_meta = yaml.load(context.text)

    backup = BackupManager(context, node).get_backup(backup_num)
    assert_that(backup.meta, has_entries(expected_meta))


@then('ch_backup entries of {node:w} are in proper condition')
def step_check_backups_conditions(context, node):
    ch_backup = BackupManager(context, node)
    backup_ids = ch_backup.get_backup_ids()

    expected_backups_count = len(context.table.rows)
    current_backups_count = len(backup_ids)
    assert_that(
        current_backups_count, equal_to(expected_backups_count),
        'Backups count = {0}, expected {1}'.format(current_backups_count,
                                                   expected_backups_count))

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
                'Backup #{0} "{1}": {2} expected {3} = {4}, but was {5}'.
                format(row['num'], row['title'], backup.name, cond,
                       expected_value, current_value))
