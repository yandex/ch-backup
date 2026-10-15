"""Steps for keyless copy, encrypted objects and legacy backup metadata."""

import json

from behave import given, then

from tests.integration.modules.backup_metadata import BackupStorage
from tests.integration.modules.steps import get_step_data


@given('a keyless copy configuration "{path}" on {node:w}')
def step_keyless_copy_config(context, path, node):
    BackupStorage(context, node).create_keyless_config(path, **get_step_data(context))


@given('full metadata of {node:w} backup "{backup_name}" uses legacy encrypted JSON')
def step_legacy_full_metadata(context, node, backup_name):
    BackupStorage(context, node).rewrite_legacy_metadata(backup_name)


@then('private definitions in {node:w} backup "{backup_name}" are encrypted')
def step_private_definitions_encrypted(context, node, backup_name):
    options = get_step_data(context)
    storage = BackupStorage(context, node)
    backup = storage.layout.get_backup(backup_name)
    assert backup is not None
    assert backup.encrypted
    ddl = storage.layout.get_table_create_statements(
        backup, options["database"], [options["table"]]
    )
    assert any(options["ddl_marker"] in sql for _, sql in ddl)
    assert any(
        options["collection_marker"]
        in storage.layout.get_named_collection_create_statement(backup, name)
        for name in backup.get_named_collections()
    )
    for path, raw in storage.raw_objects(backup_name).items():
        assert options["ddl_marker"].encode() not in raw
        assert options["collection_marker"].encode() not in raw
        if path.endswith(("backup_struct.json", "backup_light_struct.json")):
            assert json.loads(raw)["meta"]["metadata_encrypted"] is False
