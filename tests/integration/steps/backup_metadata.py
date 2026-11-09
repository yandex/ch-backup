"""Steps for keyless copy, encrypted objects and legacy backup metadata."""

import json

from behave import given, then, when

from ch_backup.clickhouse.object_storage_metadata import parse_object_storage_metadata
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


@then('all parts in {node:w} backup "{backup_name}" link to "{owner}"')
def step_part_links(context, node, backup_name, owner):
    backup = BackupStorage(context, node).layout.get_backup(backup_name)
    assert backup is not None
    parts = backup.get_parts()
    assert parts, "Expected linked parts"
    assert all(part.link == owner for part in parts), [part.link for part in parts]


@then(
    '{metadata_type:w} metadata of {node:w} backup "{backup_name}" in bucket "{bucket}" has state "{state}"'
)
# pylint: disable=too-many-positional-arguments
def step_backup_state(context, metadata_type, node, backup_name, bucket, state):
    assert metadata_type in ("full", "light"), metadata_type
    backup = BackupStorage(context, node, bucket).layout.get_backup(
        backup_name, use_light_meta=metadata_type == "light"
    )
    assert backup is not None
    assert backup.state.value == state, backup.state


@when(
    'we save the cloud generation of {node:w} backup "{backup_name}" in bucket "{bucket}"'
)
def step_save_cloud_generation(context, node, backup_name, bucket):
    backup = BackupStorage(context, node, bucket).layout.get_backup(backup_name)
    assert backup is not None and backup.cloud_generation
    context.cloud_generation = backup.cloud_generation


@when('we delete data objects of {node:w} backup "{backup_name}"')
def step_delete_backup_data(context, node, backup_name):
    storage = BackupStorage(context, node)
    prefix = f"{storage.layout.get_backup_path(backup_name)}/data/"
    paths = storage.engine.list_dir(prefix, recursive=True, absolute=True)
    assert len(paths) >= get_step_data(context).get("min_objects", 1), paths
    storage.engine.delete_files(paths)
    assert not storage.engine.list_dir(prefix, recursive=True, absolute=True)


@when('we prepend object references to {node:w} backup "{backup_name}" metadata')
def step_prepend_object_references(context, node, backup_name):
    conf = get_step_data(context)
    layout = BackupStorage(context, node).layout
    backup = layout.get_backup(backup_name)
    assert backup is not None
    path = layout.list_cloud_storage_metadata_archives(
        backup_name, conf["disk"], backup.cloud_storage.compressed, "all"
    )[0]
    files = layout.read_cloud_storage_metadata_archive(backup, path)
    saved = getattr(context, "saved_cloud_metadata", {})
    assert (node, backup_name, path) not in saved, f"Metadata already saved: {path}"
    saved[node, backup_name, path] = files.copy()
    context.saved_cloud_metadata = saved
    for index, (name, data) in enumerate(files):
        if name.endswith("frozen_metadata.txt"):
            continue
        metadata = parse_object_storage_metadata(data)
        objects = conf["objects"]
        lines = data.decode().split("\n")
        count, size = map(int, lines[1].split("\t"))
        lines[1] = (
            f"{count + len(objects)}\t{size + sum(obj['size'] for obj in objects)}"
        )
        lines[2:2] = [
            f"{obj['size']}\t"
            + (
                obj.get("full_key", obj["key"])
                if metadata.full_object_keys
                else obj["key"]
            )
            for obj in objects
        ]
        files[index] = (name, "\n".join(lines).encode())
        break
    else:
        raise AssertionError("No object metadata found")
    layout.upload_cloud_storage_metadata_files(backup, path, files)
    layout.wait()


@when("we restore saved cloud storage metadata")
def step_restore_cloud_metadata(context):
    saved = getattr(context, "saved_cloud_metadata", {})
    assert saved, "No saved cloud storage metadata to restore"
    for (node, backup_name, path), files in saved.items():
        layout = BackupStorage(context, node).layout
        backup = layout.get_backup(backup_name)
        assert backup is not None
        layout.upload_cloud_storage_metadata_files(backup, path, files)
        layout.wait()
        assert layout.read_cloud_storage_metadata_archive(backup, path) == files
    context.saved_cloud_metadata = {}


@given('local parts of {node:w} backup "{backup_name}" use the {layout:w} layout')
def step_local_part_layout(context, node, backup_name, layout):
    assert layout in {"tar", "legacy"}
    if layout == "legacy":
        BackupStorage(context, node).rewrite_per_file_parts(backup_name)


@given(
    'native cloud backups "{names}" from {source_node:w} are legacy copies on {target_node:w}'
)
def step_seed_legacy_cloud_copies(context, names, source_node, target_node):
    source = BackupStorage(context, source_node)
    target = BackupStorage(context, target_node)
    assert (
        source.config["storage"]["credentials"]["bucket"]
        != target.config["storage"]["credentials"]["bucket"]
    )
    context.legacy_cloud_objects = {}
    for name in names.split(","):
        backup = source.layout.get_backup(name)
        assert backup is not None and backup.cloud_storage.data_copied
        assert backup.cloud_generation is None
        for path, data in source.raw_objects(name).items():
            target.engine.upload_data(data, path)
            if "/disks/" in path or "/cloud_storage/" in path:
                context.legacy_cloud_objects[path] = data
        # Pre-generation copies retained native paths and recorded their source.
        backup.copied_from = {
            "endpoint_url": source.node_config["storage"]["credentials"][
                "endpoint_url"
            ],
            "bucket": source.config["storage"]["credentials"]["bucket"],
            "path_root": source.config["backup"]["path_root"],
        }
        target.layout.upload_backup_metadata(backup)
    child = target.layout.get_backup(names.split(",")[-1])
    assert child is not None and any(
        part.link == names.split(",")[0] for part in child.get_parts()
    )
    assert context.legacy_cloud_objects


@then("legacy cloud archives and payload on {node:w} are unchanged")
def step_legacy_cloud_objects_unchanged(context, node):
    storage = BackupStorage(context, node)
    for path, data in context.legacy_cloud_objects.items():
        assert storage.engine.download_data(path) == data, path
