"""External cleanup destination authorization and the disabled option."""

# pylint: disable=protected-access


import pytest

from ch_backup.backup.metadata import CopyScope, CopyScopeStatus
from ch_backup.ch_backup import _CloudCleanupPlan
from ch_backup.exceptions import ConfigurationError


@pytest.mark.parametrize(
    "configured,recorded,accepted",
    [
        (None, "https://s3.amazonaws.com/", True),
        ("https://s3.amazonaws.com", None, True),
        ("https://s3.amazonaws.com", "https://untrusted.test", False),
    ],
)
def test_cleanup_requires_configured_endpoint(cleanup, configured, recorded, accepted):
    """Accept both AWS default forms, but never send credentials to another host."""
    cleanup.config["storage"]["credentials"]["endpoint_url"] = configured
    cleanup.meta.cloud_storage.source["endpoint_url"] = recorded
    if not accepted:
        with pytest.raises(ConfigurationError, match="endpoint"):
            cleanup.manager._resolve_external_cloud_deletion_target(cleanup.meta)
        cleanup.engine_factory.assert_not_called()
        cleanup.layout.download_cloud_storage_metadata_archive.assert_not_called()
        return
    storage, prefix = cleanup.manager._resolve_external_cloud_deletion_target(
        cleanup.meta
    )
    assert storage["credentials"]["endpoint_url"] == configured
    assert prefix == "copies/backup1"


@pytest.mark.parametrize(
    "field,value",
    [
        ("copied_from", None),
        ("bucket", ""),
        ("bucket", "unexpected-bucket"),
        ("path", ""),
        ("path", "copies/other"),
        ("path", "copies/../backup1"),
        ("path", "copies/./backup1"),
    ],
)
def test_invalid_cleanup_destination_cannot_create_storage_client(
    cleanup, field, value
):
    """
    An invalid destination can expose credentials or select unrelated objects
    before deletion starts. Reject it before creating a client or reading its
    archives.
    """
    c = cleanup
    c.config["copy"]["cloud_storage"]["target"]["bucket"] = "payload"
    if field == "copied_from":
        c.meta.copied_from = value
    else:
        c.meta.cloud_storage.source[field] = value
    with pytest.raises(ConfigurationError):
        c.manager._resolve_external_cloud_deletion_target(c.meta)
    c.engine.delete_files.assert_not_called()
    c.layout.download_cloud_storage_metadata_archive.assert_not_called()
    c.engine_factory.assert_not_called()


@pytest.mark.parametrize(
    "prefix,configured_endpoint,accepted",
    [
        ("ch_backup/backup1", "https://s3.amazonaws.com", False),
        ("ch_backup/backup1/nested/backup1", "https://s3.amazonaws.com", False),
        ("ch_backup/backup1/nested/backup1", None, False),
        ("copies//backup1", "https://s3.amazonaws.com", True),
    ],
)
def test_cleanup_overlap_and_s3_key_boundaries(
    cleanup, prefix, configured_endpoint, accepted
):
    """
    Metadata removal could erase payload in a nested backup directory. Equal prefixes also overlap. Repeated slashes are literal S3 key characters.
    """
    c = cleanup
    c.config["storage"]["credentials"]["endpoint_url"] = configured_endpoint
    c.config["copy"]["cloud_storage"]["target"][
        "endpoint_url"
    ] = "https://s3.amazonaws.com"
    c.meta.cloud_storage.source.update(bucket="backups", path=prefix)
    if not accepted:
        with pytest.raises(ConfigurationError, match="overlaps"):
            c.manager._resolve_external_cloud_deletion_target(c.meta)
    else:
        assert c.manager._resolve_external_cloud_deletion_target(c.meta)[1] == prefix


def test_cleanup_uses_explicit_credentials_with_recorded_bucket(cleanup):
    """
    The copy destination can require different credentials from backup storage.
    Resolve configured credentials for the recorded endpoint while retaining
    the payload bucket recorded in metadata.
    """
    c = cleanup
    c.config["copy"]["cloud_storage"]["target"].update(
        endpoint_url="https://external.test",
        access_key_id="dummy-key",
        secret_access_key="dummy-secret",
    )
    c.meta.cloud_storage.source["endpoint_url"] = "https://external.test/"
    storage, _ = c.manager._resolve_external_cloud_deletion_target(c.meta)
    assert storage["credentials"] == {
        "endpoint_url": "https://external.test",
        "bucket": "payload",
        "access_key_id": "dummy-key",
        "secret_access_key": "dummy-secret",
    }


def test_disabled_external_cleanup_retains_payload_without_validating_target(
    cleanup, messages
):
    """Retaining external data needs no authorized deletion target, but must warn the caller."""
    c = cleanup
    c.meta.cloud_storage.source["endpoint_url"] = "https://untrusted.test"
    c.records[c.meta.name] = c.meta.dump_json()
    before = dict(c.records)
    objects = set(c.objects)

    _, _, plan = c.manager._prepare_deletion_batch([c.meta], {c.meta.name: {}}, False)[
        0
    ]

    assert plan == _CloudCleanupPlan()
    assert c.objects == objects and c.records == before
    c.layout.download_cloud_storage_metadata_archive.assert_not_called()
    c.engine_factory.assert_not_called()
    assert any(
        record["level"].name == "WARNING"
        and all(
            value in record["message"]
            for value in ("backup1", "payload", "copies/backup1")
        )
        for record in messages
    )


@pytest.mark.parametrize(
    "scopes",
    [
        {CopyScope.CLOUD_STORAGE: CopyScopeStatus.PENDING},
        {CopyScope.FILES: CopyScopeStatus.COMPLETED},
    ],
)
def test_files_only_copy_with_no_manifest_preserves_source_objects(cleanup, scopes):
    c = cleanup
    c.meta.copy_scopes = scopes
    c.meta.cloud_storage.source["endpoint_url"] = "https://source.example"
    assert (
        c.manager._prepare_backup_cloud_cleanup(c.meta, {}, True) == _CloudCleanupPlan()
    )
    c.engine_factory.assert_not_called()


@pytest.mark.parametrize("explicit", [False, True])
def test_delete_refuses_existing_shared_directory_before_mutation(cleanup, explicit):
    c = cleanup
    c.meta.cloud_storage.source.update(bucket="backups", path="ch_backup/backup1")
    c.records[c.meta.name] = c.meta.dump_json()
    c.objects.update({"ch_backup/backup1/part", "ch_backup/backup1/unrelated"})
    records, objects = c.records.copy(), c.objects.copy()
    with pytest.raises(ConfigurationError, match="overlaps"):
        c.manager.delete(
            c.meta.name, purge_partial=False, delete_cloud_storage_data=explicit
        )
    assert c.records == records
    assert c.objects == objects
    c.layout.delete_backup.assert_not_called()
    c.layout.upload_backup_metadata.assert_not_called()
