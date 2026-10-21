"""
Steps for interacting with S3.
"""

from behave import given, then, when
from hamcrest import assert_that, equal_to

from ch_backup.clickhouse.object_storage_metadata import parse_object_storage_metadata
from ch_backup.util import CLOUD_STORAGE_EXCLUDE_FILE_NAMES
from tests.integration.modules import s3
from tests.integration.modules.clickhouse import ClickhouseClient, get_part_path
from tests.integration.modules.docker import get_container
from tests.integration.modules.steps import get_step_data


@given("a working s3")
def step_wait_for_s3_alive(context):
    """
    Ensure that s3 is ready to accept incoming requests, and
    the buckets specified in the config are created.
    """
    s3.wait_for_s3_buckets(context)


@then("s3 contains {count:d} objects")
def step_s3_contains_files(context, count):
    s3_client = s3.S3Client(context)
    objects = s3_client.list_objects("")
    assert_that(
        len(objects),
        equal_to(count),
        f"Objects count = {len(objects)}, expected {count}, objects {objects}",
    )


@then("s3 bucket {bucket} contains {count:d} objects")
def step_cloud_storage_bucket_contains_files(context, bucket, count):
    s3_client = s3.S3Client(context, bucket)
    objects = s3_client.list_objects("")
    assert_that(
        len(objects),
        equal_to(count),
        f"Objects count = {len(objects)}, expected {count}, objects {objects}",
    )


@then('s3 bucket {bucket} contains objects with prefix "{prefix}"')
def step_cloud_storage_bucket_contains_prefix(context, bucket, prefix):
    s3_client = s3.S3Client(context, bucket)
    objects = s3_client.list_objects(prefix)
    assert_that(
        len(objects) > 0,
        equal_to(True),
        f"No objects with prefix {prefix} in bucket {bucket}",
    )


@then('s3 bucket {bucket} contains no objects with prefix "{prefix}"')
def step_cloud_storage_bucket_has_no_prefix(context, bucket, prefix):
    s3_client = s3.S3Client(context, bucket)
    objects = s3_client.list_objects(prefix)
    assert_that(
        objects,
        equal_to([]),
        f"Unexpected objects with prefix {prefix} in bucket {bucket}: {objects}",
    )


@when("we delete all objects in s3 bucket {bucket}")
def step_delete_all_objects_in_bucket(context, bucket):
    s3_client = s3.S3Client(context, bucket)
    for path in s3_client.list_objects(""):
        s3_client.delete_data(path)
    assert_that(s3_client.list_objects(""), equal_to([]))


@when("we put object in S3")
def step_create_file_in_s3(context):
    conf = get_step_data(context)
    s3_client = s3.S3Client(context, conf["bucket"])
    s3_client.upload_data(conf["data"], conf["path"])
    assert s3_client.path_exists(conf["path"])


@when("we delete object in S3")
def stop_delete_file_in_S3(context):
    conf = get_step_data(context)
    s3_client = s3.S3Client(context, conf["bucket"])
    s3_client.delete_data(conf["path"])
    assert not s3_client.path_exists(conf["path"])


@then("an object in S3 contains")
def step_s3_object_contents(context):
    conf = get_step_data(context)
    data = s3.S3Client(context, conf["bucket"]).download_data(conf["path"])
    assert (
        data == conf["data"].encode()
    ), f"Unexpected contents of {conf['path']}: {data!r}"


def _read_object_keys(container, path, prefix):
    result = container.exec_run(["cat", path], user="root")
    assert result.exit_code == 0, result.output.decode(errors="replace")
    metadata = parse_object_storage_metadata(result.output)
    return [
        (
            obj.key
            if metadata.full_object_keys
            else f"{prefix.strip('/')}/{obj.key}".lstrip("/")
        )
        for obj in metadata.objects
    ]


@when("we save and delete the S3 objects of a data part file on {node:w}")
def step_save_and_delete_part_file_objects(context, node):
    """Temporarily remove the objects behind one active part's metadata file."""
    conf = get_step_data(context)
    path = get_part_path(context, node, conf["database"], conf["table"], conf["part"])
    keys = _read_object_keys(
        get_container(context, node), f"{path}/{conf['file']}", conf["path"]
    )
    assert keys, "The file has no S3 objects to delete"
    client = s3.S3Client(context, conf["bucket"])
    saved = getattr(context, "saved_s3_objects", {})
    # Save every object before deleting any of them. Metadata can reference
    # multiple objects, and old versions store keys relative to the disk prefix.
    for key in keys:
        assert (conf["bucket"], key) not in saved, f"Object already saved: {key}"
        saved[conf["bucket"], key] = client.download_data(key)
    context.saved_s3_objects = saved
    for key in keys:
        client.delete_data(key)
        assert not client.path_exists(key), f"Object was not deleted: {key}"


@when("we restore saved S3 objects")
def step_restore_saved_objects(context):
    saved = getattr(context, "saved_s3_objects", {})
    assert saved, "No saved S3 objects to restore"
    for (bucket, key), data in saved.items():
        client = s3.S3Client(context, bucket)
        client.upload_data(data, key)
        assert client.download_data(key) == data, f"Object was not restored: {key}"
    context.saved_s3_objects = {}


@when("we save the S3 objects of a frozen data part on {node:w}")
def step_save_frozen_part_objects(context, node):
    """Snapshot the backup's source objects, including their original bytes."""
    conf = get_step_data(context)
    disk_path = (
        ClickhouseClient(context, node)
        .get_response(
            f"SELECT path FROM system.disks WHERE name = '{conf['disk']}' FORMAT TSVRaw"
        )
        .strip()
    )
    assert disk_path, f"Disk not found: {conf['disk']}"
    container = get_container(context, node)
    result = container.exec_run(
        [
            "find",
            f"{disk_path}/shadow/{conf['backup']}",
            "-type",
            "d",
            "-name",
            conf["part"],
        ],
        user="root",
    )
    assert result.exit_code == 0, result.output
    paths = result.output.decode().splitlines()
    assert len(paths) == 1, f"Expected one frozen part, found paths: {paths}"
    path = paths[0]
    result = container.exec_run(
        ["find", path, "-maxdepth", "1", "-type", "f"], user="root"
    )
    assert result.exit_code == 0, result.output
    keys = {
        key
        for filename in result.output.decode().splitlines()
        if filename.rsplit("/", 1)[-1] not in CLOUD_STORAGE_EXCLUDE_FILE_NAMES
        for key in _read_object_keys(container, filename, conf["path"])
    }
    assert keys, f"No source objects referenced by {path}"
    client = s3.S3Client(context, conf["bucket"])
    context.s3_object_snapshot = {
        (conf["bucket"], key): client.download_data(key) for key in keys
    }


@then("saved S3 objects are unchanged")
def step_saved_s3_objects_unchanged(context):
    saved = context.s3_object_snapshot
    assert saved, "No source object snapshot"
    for (bucket, key), data in saved.items():
        client = s3.S3Client(context, bucket)
        assert client.path_exists(key), f"Source object was deleted: {bucket}/{key}"
        assert (
            client.download_data(key) == data
        ), f"Source object changed: {bucket}/{key}"
