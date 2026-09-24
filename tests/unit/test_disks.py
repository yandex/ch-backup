"""
Unit tests disks module.
"""

# Both disk managers of the module are covered here.
# pylint: disable=too-many-lines

import copy
import io
import os
import tempfile
import threading
import time
import unittest
import unittest.mock
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Iterator, Sequence

import pytest
import xmltodict

from ch_backup.backup.layout import BackupLayout
from ch_backup.backup.metadata import PartMetadata
from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.disks import (
    CH_DISK_CONFIG_PATH,
    ClickHouseBackupDisks,
    ClickHouseDisksException,
    ClickHouseTemporaryDisks,
    _open_config_file,
    _render_disks_config,
)
from ch_backup.clickhouse.models import Disk, FrozenPart, Table
from ch_backup.config import DEFAULT_CONFIG, Config
from tests.unit.utils import assert_equal, parametrize


@parametrize(
    {
        "id": "No timeout",
        "args": {
            "clickhouse_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
            "disk_name": "object_storage",
            "source": {
                "endpoint": "localhost",
                "bucket": "test-bucket",
                "path": "cluster1/shard1/",
            },
            "temp_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage_source>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>3600000</request_timeout_ms>
                      <skip_access_check>true</skip_access_check>
                    </object_storage_source>
                    <object_storage>
                      <request_timeout_ms replace="replace">3600000</request_timeout_ms>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
        },
    },
    {
        "id": "Small timeout",
        "args": {
            "clickhouse_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>30000</request_timeout_ms>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
            "disk_name": "object_storage",
            "source": {
                "endpoint": "localhost",
                "bucket": "test-bucket",
                "path": "cluster1/shard1/",
            },
            "temp_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage_source>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>3600000</request_timeout_ms>
                      <skip_access_check>true</skip_access_check>
                    </object_storage_source>
                    <object_storage>
                      <request_timeout_ms replace="replace">3600000</request_timeout_ms>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
        },
    },
    {
        "id": "Large timeout",
        "args": {
            "clickhouse_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>7200000</request_timeout_ms>
                    </object_storage>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
            "disk_name": "object_storage",
            "source": {
                "endpoint": "localhost",
                "bucket": "test-bucket",
                "path": "cluster1/shard1/",
            },
            "temp_config": """
              <clickhouse>
                <storage_configuration>
                  <disks>
                    <object_storage_source>
                      <type>s3</type>
                      <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                      <access_key_id>AKIAACCESSKEY</access_key_id>
                      <secret_access_key>SecretAccesskey</secret_access_key>
                      <request_timeout_ms>7200000</request_timeout_ms>
                      <skip_access_check>true</skip_access_check>
                    </object_storage_source>
                  </disks>
                </storage_configuration>
              </clickhouse>
              """,
        },
    },
)
def test_temporary_disk(clickhouse_config, disk_name, source, temp_config):
    context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
    context.ch_ctl = unittest.mock.MagicMock()
    context.backup_layout = unittest.mock.MagicMock()
    context.backup_meta = unittest.mock.MagicMock()
    context.backup_meta.cloud_storage.data_copied = False
    with unittest.mock.patch(
        "builtins.open",
        new=unittest.mock.mock_open(read_data=clickhouse_config),
        create=True,
    ):
        with unittest.mock.patch("yaml.load", return_value=""):
            context.ch_config = ClickhouseConfig(Config("foo"))
        context.ch_config.load()
    disk = ClickHouseTemporaryDisks(
        context.ch_ctl,
        context.backup_layout,
        context.config_root,
        context.backup_meta,
        source["bucket"],
        source["path"],
        source["endpoint"],
        context.ch_config,
    )

    with _capture_config_files() as (written, _):
        # pylint: disable=protected-access
        # Initialise _disks the same way __enter__ does
        disk._disks = (context.ch_config.config.get("storage_configuration") or {}).get(
            "disks"
        ) or {}
        disk._create_temporary_disk(
            context.backup_meta,
            disk_name,
        )

    config_path = (
        f"/etc/clickhouse-server/config.d/cloud_storage_tmp_disk_{disk_name}_source.xml"
    )
    expected_content = xmltodict.parse(temp_config, disable_entities=False)
    actual_content = xmltodict.parse(written[config_path], disable_entities=False)
    assert_equal(actual_content, expected_content)


BACKUP_STORAGE_CREDENTIALS = {
    "endpoint_url": "https://minio:9000/",
    "access_key_id": "BackupAccessKey",
    "secret_access_key": "BackupSecretKey",
    "bucket": "backup-bucket",
}


def _make_backup_storage_config() -> dict:
    """Helper: build a config with credentials of the backup storage."""
    config: dict = copy.deepcopy(DEFAULT_CONFIG)
    config["backup"]["path_root"] = "ch_backup/"
    config["storage"]["credentials"] = copy.deepcopy(BACKUP_STORAGE_CREDENTIALS)
    return config


def _make_backup_layout(config: dict) -> unittest.mock.MagicMock:
    """Helper: mock a BackupLayout that still builds real backup paths."""
    with (
        unittest.mock.patch("ch_backup.backup.layout.StorageLoader"),
        unittest.mock.patch("ch_backup.backup.layout.get_encryption") as get_encryption,
    ):
        get_encryption.return_value.metadata_size.return_value = 0
        real_layout = BackupLayout(config)  # type: ignore[arg-type]

    layout = unittest.mock.MagicMock()
    layout.get_cloud_storage_data_path.side_effect = (
        real_layout.get_cloud_storage_data_path
    )
    return layout


def _make_context(config: dict, clickhouse_config_xml: str) -> BackupContext:
    """Helper: build a BackupContext with a loaded ClickHouse configuration."""
    context = BackupContext(config)  # type: ignore[arg-type]
    context.ch_ctl = unittest.mock.MagicMock()
    context.backup_meta = unittest.mock.MagicMock()
    context.backup_meta.name = "20260101T000000"
    context.backup_meta.get_sanitized_name.return_value = "20260101T000000"
    with unittest.mock.patch(
        "builtins.open",
        new=unittest.mock.mock_open(read_data=clickhouse_config_xml),
        create=True,
    ):
        with unittest.mock.patch("yaml.load", return_value=""):
            context.ch_config = ClickhouseConfig(Config("foo"))
        context.ch_config.load()
    return context


def _make_linked_backup_meta(backup_name: str) -> unittest.mock.MagicMock:
    """Helper: mock metadata of a backup holding data of deduplicated parts."""
    backup_meta = unittest.mock.MagicMock()
    backup_meta.name = backup_name
    backup_meta.get_sanitized_name.return_value = backup_name
    backup_meta.cloud_storage.data_copied = True
    return backup_meta


# pylint: disable=too-many-positional-arguments
def _make_temporary_disks(
    clickhouse_config_xml: str,
    cloud_storage_disks: list[str] | None = None,
    data_copied: bool = False,
    source_bucket: str | None = "test-bucket",
    parts: Sequence[PartMetadata] = (),
) -> ClickHouseTemporaryDisks:
    """Helper: build ClickHouseTemporaryDisks with mocked dependencies."""
    context = _make_context(_make_backup_storage_config(), clickhouse_config_xml)
    context.backup_layout = _make_backup_layout(_make_backup_storage_config())
    context.backup_layout.get_backup.side_effect = lambda name, **_: (
        _make_linked_backup_meta(name)
    )
    context.backup_meta.cloud_storage.disks = cloud_storage_disks or []  # type: ignore[misc]
    context.backup_meta.cloud_storage.enabled = bool(cloud_storage_disks)  # type: ignore[misc]
    context.backup_meta.cloud_storage.data_copied = data_copied  # type: ignore[misc]
    context.backup_meta.cloud_storage.requires_source_bucket = (  # type: ignore[misc]
        bool(cloud_storage_disks) and not data_copied
    )

    table = unittest.mock.MagicMock()
    table.database = "db1"
    table.name = "table1"
    table.get_parts.return_value = list(parts)
    context.backup_meta.get_databases.return_value = ["db1"]  # type: ignore[attr-defined]
    context.backup_meta.get_tables.return_value = [table]  # type: ignore[attr-defined]

    return ClickHouseTemporaryDisks(
        context.ch_ctl,
        context.backup_layout,
        context.config_root,
        context.backup_meta,
        source_bucket,
        None,
        None,
        context.ch_config,
    )


def test_enter_without_storage_configuration():
    """
    __enter__ must not raise KeyError when the ClickHouse config has no
    storage_configuration section (valid CH config that uses the default disk).
    """
    clickhouse_config_xml = """
        <clickhouse>
            <logger>
                <level>trace</level>
            </logger>
        </clickhouse>
    """
    disk_manager = _make_temporary_disks(clickhouse_config_xml, cloud_storage_disks=[])

    with _capture_config_files() as (written, _):
        with disk_manager:
            # pylint: disable=protected-access
            assert disk_manager._disks == {}

    actual_content = xmltodict.parse(
        written[CH_DISK_CONFIG_PATH], disable_entities=False
    )
    assert_equal(
        actual_content["clickhouse"]["history-file"],
        "/tmp/.disks-file-history",
    )


def test_create_temporary_disk_missing_disk_raises():
    """
    _create_temporary_disk must raise ClickHouseDisksException with a descriptive
    message when disk_name is present in backup cloud storage metadata but absent
    from the ClickHouse storage_configuration.
    """
    clickhouse_config_xml = """
        <clickhouse>
            <storage_configuration>
                <disks>
                    <other_disk>
                        <type>s3</type>
                        <endpoint>https://localhost/bucket/path/</endpoint>
                    </other_disk>
                </disks>
            </storage_configuration>
        </clickhouse>
    """
    disk_manager = _make_temporary_disks(
        clickhouse_config_xml, cloud_storage_disks=["missing_disk"]
    )

    with unittest.mock.patch("builtins.open", new=unittest.mock.mock_open()):
        # Manually initialise _disks as __enter__ would
        # pylint: disable=protected-access
        disk_manager._disks = (
            disk_manager._ch_config.config.get("storage_configuration") or {}
        ).get("disks") or {}  # fmt: skip

        try:
            disk_manager._create_temporary_disk(
                disk_manager._backup_meta,
                "missing_disk",
            )
            assert False, "Expected ClickHouseDisksException was not raised"
        except ClickHouseDisksException as exc:
            assert "missing_disk" in str(exc)
            assert "storage_configuration" in str(exc)


BACKUP_DISK_CLICKHOUSE_CONFIG = """
    <clickhouse>
        <storage_configuration>
            <disks>
                <object_storage>
                    <type>s3</type>
                    <endpoint>https://localhost/test-bucket/cluster1/shard1/</endpoint>
                    <access_key_id>AKIAACCESSKEY</access_key_id>
                    <secret_access_key>SecretAccesskey</secret_access_key>
                </object_storage>
            </disks>
        </storage_configuration>
    </clickhouse>
"""

BACKUP_DISK_PATH = "/var/lib/clickhouse/disks/object_storage_backup/"
BACKUP_DISK_CONFIG_PATH = (
    "/etc/clickhouse-server/config.d/cloud_storage_tmp_disk_object_storage_backup.xml"
)


def _make_backup_disks(
    clickhouse_config_xml: str,
    storage_config: dict | None = None,
) -> tuple[ClickHouseBackupDisks, unittest.mock.MagicMock]:
    """Helper: build ClickHouseBackupDisks with mocked dependencies."""
    config = _make_backup_storage_config()
    config["storage"].update(storage_config or {})
    context = _make_context(config, clickhouse_config_xml)
    context.ch_ctl.get_disk.return_value = Disk(  # type: ignore[attr-defined]
        "object_storage_backup", BACKUP_DISK_PATH, "s3"
    )
    disks = ClickHouseBackupDisks(
        context.ch_ctl,
        _make_backup_layout(config),
        context.config_root,
        context.backup_meta,
        context.ch_config,
    )
    return disks, context.ch_ctl  # type: ignore[return-value]


@contextmanager
def _capture_config_files() -> Iterator[tuple[dict[str, str], unittest.mock.MagicMock]]:
    """
    Helper: collect content of rendered config files and calls removing them.

    Keeps unit tests off the filesystem, which the disks module writes to directly.
    """
    written: dict[str, str] = {}

    @contextmanager
    def collect(path: str) -> Iterator[IO[str]]:
        buffer = io.StringIO()
        yield buffer
        written[path] = buffer.getvalue()

    with unittest.mock.patch(
        "ch_backup.clickhouse.disks._open_config_file", new=collect
    ):
        with unittest.mock.patch("os.remove") as remove_mock:
            yield written, remove_mock


def test_backup_disk_config():
    """
    create_disk() must point the temporary disk to the backup bucket and use
    the credentials of the backup storage, not the ones of the source disk.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    expected_config = """
        <clickhouse>
            <storage_configuration>
                <disks>
                    <object_storage_backup>
                        <type>s3</type>
                        <endpoint>https://minio:9000/backup-bucket/ch_backup/20260101T000000/cloud_storage/object_storage/</endpoint>
                        <access_key_id>BackupAccessKey</access_key_id>
                        <secret_access_key>BackupSecretKey</secret_access_key>
                        <skip_access_check>true</skip_access_check>
                        <request_timeout_ms>3600000</request_timeout_ms>
                    </object_storage_backup>
                    <object_storage>
                        <request_timeout_ms replace="replace">3600000</request_timeout_ms>
                    </object_storage>
                </disks>
            </storage_configuration>
        </clickhouse>
    """

    with _capture_config_files() as (written, _):
        with disk_manager:
            disk_manager.create_disk("object_storage")

    assert_equal(
        xmltodict.parse(written[BACKUP_DISK_CONFIG_PATH], disable_entities=False),
        xmltodict.parse(expected_config, disable_entities=False),
    )


def _created_disk_config(
    storage_config: dict,
) -> dict:
    """Helper: build a backup disk and return its rendered configuration."""
    disk_manager, _ = _make_backup_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG, storage_config=storage_config
    )
    with _capture_config_files() as (written, _):
        with disk_manager:
            disk_manager.create_disk("object_storage")

    return xmltodict.parse(written[BACKUP_DISK_CONFIG_PATH], disable_entities=False)[
        "clickhouse"
    ]["storage_configuration"]["disks"]["object_storage_backup"]


def test_backup_disk_gets_its_own_metadata_path():
    """
    A temporary disk sharing the metadata path of the disk it is built from
    would have its cleanup remove the frozen data of that disk.
    """
    clickhouse_config_xml = BACKUP_DISK_CLICKHOUSE_CONFIG.replace(
        "<type>s3</type>",
        "<type>s3</type><metadata_path>/var/lib/clickhouse/disks/object_storage/</metadata_path>",
    )
    disk_manager, _ = _make_backup_disks(clickhouse_config_xml)

    with _capture_config_files() as (written, _):
        with disk_manager:
            disk_manager.create_disk("object_storage")

    disk_config = xmltodict.parse(
        written[BACKUP_DISK_CONFIG_PATH], disable_entities=False
    )["clickhouse"]["storage_configuration"]["disks"]["object_storage_backup"]
    assert "metadata_path" not in disk_config


def test_backup_disk_endpoint_follows_virtual_addressing_style():
    """
    With virtual addressing the bucket is a part of the host name, addressing it
    as a path would not resolve.
    """
    disk_config = _created_disk_config(
        {"boto_config": {"addressing_style": "virtual", "region_name": "us-east-1"}}
    )

    assert_equal(
        disk_config["endpoint"],
        "https://backup-bucket.minio:9000/ch_backup/20260101T000000/cloud_storage/object_storage/",
    )


def test_backup_disk_uses_the_proxy_resolver_of_the_backup_storage():
    """
    ClickHouse must reach the backup storage the same way ch-backup does.

    The resolver is passed on instead of a host resolved once, so that
    ClickHouse picks a proxy per request and drops one that fails.
    """
    disk_config = _created_disk_config(
        {"proxy_resolver": {"uri": "http://resolver/hostname", "proxy_port": 8080}}
    )

    assert_equal(
        disk_config["proxy"],
        {
            "resolver": {
                "endpoint": "http://resolver/hostname",
                "proxy_scheme": "http",
                "proxy_port": "8080",
            }
        },
    )


def test_backup_disk_proxy_resolver_endpoint_always_has_a_path():
    """
    ClickHouse builds the resolver request line out of the endpoint path
    alone, so an empty path makes every resolve request malformed.
    """
    disk_config = _created_disk_config(
        {"proxy_resolver": {"uri": "http://resolver:8080", "proxy_port": 4080}}
    )

    assert_equal(disk_config["proxy"]["resolver"]["endpoint"], "http://resolver:8080/")


def test_backup_disk_has_no_proxy_without_a_resolver():
    """
    Proxy is optional, an unset resolver must not end up in the configuration.
    """
    assert "proxy" not in _created_disk_config({})


def test_backup_disk_keeps_a_larger_request_timeout():
    """
    A timeout configured by the user must not be lowered.
    """
    clickhouse_config = BACKUP_DISK_CLICKHOUSE_CONFIG.replace(
        "</object_storage>",
        "<request_timeout_ms>7200000</request_timeout_ms></object_storage>",
    )
    disk_manager, _ = _make_backup_disks(clickhouse_config)

    with _capture_config_files() as (written, _):
        with disk_manager:
            disk_manager.create_disk("object_storage")

    disks = xmltodict.parse(written[BACKUP_DISK_CONFIG_PATH], disable_entities=False)[
        "clickhouse"
    ]["storage_configuration"]["disks"]

    assert_equal(disks["object_storage_backup"]["request_timeout_ms"], "7200000")
    assert "object_storage" not in disks


def test_backup_disk_is_added_to_clickhouse_disks_config():
    """
    The clickhouse-disks utility gets its own config, so it must list both the
    source disk and the created one, otherwise the copy command cannot run.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files() as (written, _):
        with disk_manager:
            disk_manager.create_disk("object_storage")

    disks = xmltodict.parse(
        written["/tmp/clickhouse-disks-config.xml"], disable_entities=False
    )["clickhouse"]["storage_configuration"]["disks"]

    assert sorted(disks) == ["object_storage", "object_storage_backup"]


def test_backup_disk_source_disk_is_not_modified():
    """
    create_disk() must not touch the configuration of the source disk.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files():
        with disk_manager:
            disk_manager.create_disk("object_storage")
            # pylint: disable=protected-access
            source_config = disk_manager._disks["object_storage"]

    assert_equal(
        source_config["endpoint"], "https://localhost/test-bucket/cluster1/shard1/"
    )
    assert_equal(source_config["access_key_id"], "AKIAACCESSKEY")


def test_backup_disk_is_created_once():
    """
    Repeated calls must reuse the disk instead of reloading the configuration.
    """
    disk_manager, ch_ctl = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files():
        with disk_manager:
            first = disk_manager.create_disk("object_storage")
            second = disk_manager.create_disk("object_storage")
            assert ch_ctl.reload_config.call_count == 1

    assert first is second


def test_backup_disk_is_created_once_from_several_threads():
    """
    Tables are copied in parallel, so a disk shared by them must be registered
    in ClickHouse once.
    """
    disk_manager, ch_ctl = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)
    ch_ctl.reload_config.side_effect = lambda: time.sleep(0.05)
    created: list[Disk] = []

    def create_disk() -> None:
        created.append(disk_manager.create_disk("object_storage"))

    with _capture_config_files():
        with disk_manager:
            threads = [threading.Thread(target=create_disk) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            assert ch_ctl.reload_config.call_count == 1

    assert created[0] is created[1]


def test_config_file_is_replaced_atomically():
    """
    Config files are read while further disks are being added, so a file must
    never be seen half-written.
    """
    with tempfile.TemporaryDirectory() as config_dir:
        path = os.path.join(config_dir, "disks.xml")
        _render_disks_config(path, {"object_storage": {"type": "s3"}})

        with _open_config_file(path) as f:
            f.write("<clickhouse/>")
            assert "object_storage" in Path(path).read_text(encoding="utf-8")

        assert Path(path).read_text(encoding="utf-8") == "<clickhouse/>"
        assert os.listdir(config_dir) == ["disks.xml"]


def test_backup_disk_missing_disk_raises():
    """
    create_disk() must raise ClickHouseDisksException when the disk is absent
    from the ClickHouse storage_configuration.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files():
        with disk_manager:
            try:
                disk_manager.create_disk("missing_disk")
                assert False, "Expected ClickHouseDisksException was not raised"
            except ClickHouseDisksException as exc:
                assert "missing_disk" in str(exc)
                assert "storage_configuration" in str(exc)


def test_backup_disk_is_cleaned_up_on_exit():
    """
    Leaving the context must remove the generated config files, tell ClickHouse
    to forget the disk and drop the local metadata written while copying.
    """
    disk_manager, ch_ctl = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files() as (_, remove_mock):
        with unittest.mock.patch("shutil.rmtree") as rmtree_mock:
            with disk_manager:
                disk_manager.create_disk("object_storage")

    assert_equal(
        [call.args[0] for call in remove_mock.call_args_list],
        [BACKUP_DISK_CONFIG_PATH, CH_DISK_CONFIG_PATH],
    )
    rmtree_mock.assert_called_once_with(os.path.join(BACKUP_DISK_PATH, "shadow"))
    assert ch_ctl.reload_config.call_count == 2
    # pylint: disable=protected-access
    assert "object_storage_backup" not in disk_manager._disks


def test_backup_disk_cleanup_on_error_keeps_local_data_only():
    """
    Disk configurations hold credentials of the backup storage and are removed
    even after an error, while local data is kept for investigation.

    ClickHouse is not asked to reload its configuration, since it may well be
    the reason for the error.
    """
    disk_manager, ch_ctl = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)

    with _capture_config_files() as (_, remove_mock):
        with unittest.mock.patch("shutil.rmtree") as rmtree_mock:
            with pytest.raises(ValueError):
                with disk_manager:
                    disk_manager.create_disk("object_storage")
                    raise ValueError("copy failed")

    removed = [call.args[0] for call in remove_mock.call_args_list]
    assert removed == [BACKUP_DISK_CONFIG_PATH, CH_DISK_CONFIG_PATH]
    rmtree_mock.assert_not_called()
    ch_ctl.reload_config.assert_called_once()


def test_copy_table_data_copies_frozen_shadow_directory():
    """
    Data must be copied from the shadow directory of the source disk to the
    same path on the backup disk, so that uploaded metadata keeps its layout.
    """
    disk_manager, _ = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)
    source_disk = Disk("object_storage", "/var/lib/clickhouse/disks/s3/", "s3")
    table = Table(
        "db1",
        "table1",
        "MergeTree",
        [source_disk],
        ["/var/lib/clickhouse/disks/s3/store/abc/abcdef"],
        "",
        "",
        "some-uuid",
    )

    with _capture_config_files():
        with unittest.mock.patch("ch_backup.clickhouse.disks._exec") as exec_mock:
            with unittest.mock.patch("os.makedirs") as makedirs_mock:
                with unittest.mock.patch("shutil.rmtree"):
                    with disk_manager:
                        disk = disk_manager.copy_table_data("object_storage", table)

    assert disk.name == "object_storage_backup"
    command_args = exec_mock.call_args.kwargs["command_args"]
    assert "object_storage" in command_args
    assert "object_storage_backup" in command_args
    assert (
        command_args.count("shadow/20260101T000000/store/abc/abcdef/") == 2
    ), command_args
    # The last directory is left to clickhouse-disks, see copy_table_data
    makedirs_mock.assert_called_once_with(
        os.path.join(BACKUP_DISK_PATH, "shadow/20260101T000000/store/abc"),
        exist_ok=True,
    )


def _remove_frozen_part(
    part_path: str, new_disks_interface: bool = True
) -> unittest.mock.MagicMock:
    """Helper: remove a frozen part of a disk and return the mocked call."""
    disk_manager, ch_ctl = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)
    ch_ctl.ch_version_ge.return_value = new_disks_interface
    source_disk = Disk("object_storage", "/var/lib/clickhouse/disks/s3/", "s3")
    part = FrozenPart(
        "db1", "table1", "all_1_1_0", "object_storage", part_path, "checksum", 1024, []
    )

    with _capture_config_files():
        with unittest.mock.patch("ch_backup.clickhouse.disks._exec") as exec_mock:
            with unittest.mock.patch("shutil.rmtree"):
                with disk_manager:
                    disk_manager.remove_frozen_part(source_disk, part)

    return exec_mock


@pytest.mark.parametrize(
    "new_disks_interface", [True, False], ids=["since 24.7", "before 24.7"]
)
def test_remove_frozen_part_goes_through_the_disk(new_disks_interface):
    """
    The reference count kept in the metadata of the objects is decremented
    only when the part is removed through the disk. Removal of a directory
    has to be asked for explicitly since 24.7.
    """
    exec_mock = _remove_frozen_part(
        "/var/lib/clickhouse/disks/s3/shadow/20260101T000000/store/abc/abcdef/all_1_1_0",
        new_disks_interface=new_disks_interface,
    )

    command_args = exec_mock.call_args.kwargs["command_args"]
    assert exec_mock.call_args.kwargs["command"].endswith("remove")
    assert "object_storage" in exec_mock.call_args.kwargs["common_args"]
    assert "shadow/20260101T000000/store/abc/abcdef/all_1_1_0" in command_args
    assert ("--recursive" in command_args) is new_disks_interface


def test_remove_frozen_parts_removes_every_part_on_its_own_disk():
    """
    A batch of deduplicated parts is removed in parallel, each through the
    disk it is frozen on.
    """
    disk_manager, ch_ctl = _make_backup_disks(BACKUP_DISK_CLICKHOUSE_CONFIG)
    ch_ctl.ch_version_ge.return_value = False
    disks = {
        name: Disk(name, f"/var/lib/clickhouse/disks/{name}/", "s3")
        for name in ("object_storage", "object_storage_second")
    }
    parts = [
        FrozenPart(
            "db1",
            "table1",
            f"all_{i}_{i}_0",
            name,
            f"/var/lib/clickhouse/disks/{name}/shadow/20260101T000000/all_{i}_{i}_0",
            "checksum",
            1024,
            [],
        )
        for i, name in enumerate(disks)
    ]

    with _capture_config_files():
        with unittest.mock.patch("ch_backup.clickhouse.disks._exec") as exec_mock:
            with unittest.mock.patch("shutil.rmtree"):
                with disk_manager:
                    disk_manager.remove_frozen_parts(disks, parts)

    removed = {
        call.kwargs["common_args"][-1]: call.kwargs["command_args"]
        for call in exec_mock.call_args_list
    }
    assert removed == {
        "object_storage": ["shadow/20260101T000000/all_0_0_0"],
        "object_storage_second": ["shadow/20260101T000000/all_1_1_0"],
    }


def test_remove_frozen_part_outside_shadow_raises():
    """
    Removal through the disk deletes the objects of a part that is not frozen.
    """
    with pytest.raises(ClickHouseDisksException):
        _remove_frozen_part(
            "/var/lib/clickhouse/disks/s3/store/abc/abcdef/all_1_1_0",
        )


def test_restore_requires_source_bucket_when_data_is_not_copied():
    """
    A backup that only references the source bucket cannot be restored without it.
    """
    try:
        _make_temporary_disks(
            BACKUP_DISK_CLICKHOUSE_CONFIG,
            cloud_storage_disks=["object_storage"],
            source_bucket=None,
        )
        assert False, "Expected RuntimeError was not raised"
    except RuntimeError as exc:
        assert "cloud-storage-source-bucket" in str(exc)


def test_restore_reads_copied_data_from_the_backup_bucket():
    """
    When data is copied into the backup, the temporary disk must point to the
    backup bucket and use the credentials of the backup storage.
    """
    disk_manager = _make_temporary_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG,
        cloud_storage_disks=["object_storage"],
        data_copied=True,
        source_bucket=None,
    )

    expected_config = """
        <clickhouse>
            <storage_configuration>
                <disks>
                    <object_storage_source>
                        <type>s3</type>
                        <endpoint>https://minio:9000/backup-bucket/ch_backup/20260101T000000/cloud_storage/object_storage/</endpoint>
                        <access_key_id>BackupAccessKey</access_key_id>
                        <secret_access_key>BackupSecretKey</secret_access_key>
                        <request_timeout_ms>3600000</request_timeout_ms>
                        <skip_access_check>true</skip_access_check>
                    </object_storage_source>
                    <object_storage>
                        <request_timeout_ms replace="replace">3600000</request_timeout_ms>
                    </object_storage>
                </disks>
            </storage_configuration>
        </clickhouse>
    """

    with _capture_config_files() as (written, _):
        with disk_manager:
            pass

    config_path = "/etc/clickhouse-server/config.d/cloud_storage_tmp_disk_object_storage_source.xml"
    assert_equal(
        xmltodict.parse(written[config_path], disable_entities=False),
        xmltodict.parse(expected_config, disable_entities=False),
    )


LINKED_BACKUP_NAME = "20251231T000000"


def _make_linked_part(
    name: str = "all_1_1_0", link_part_name: str | None = None
) -> PartMetadata:
    """Helper: build metadata of a deduplicated part on a cloud storage disk."""
    return PartMetadata(
        database="db1",
        table="table1",
        name=name,
        checksum="checksum",
        size=1024,
        files=["checksums.txt"],
        tarball=True,
        link=LINKED_BACKUP_NAME,
        link_part_name=link_part_name,
        disk_name="object_storage",
    )


def test_restore_creates_a_disk_per_backup_holding_data():
    """
    Data of a deduplicated part is left in the backup it was copied into, so
    it is read through a temporary disk of that backup.
    """
    disk_manager = _make_temporary_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG,
        cloud_storage_disks=["object_storage"],
        data_copied=True,
        source_bucket=None,
        parts=[_make_linked_part()],
    )

    with _capture_config_files() as (written, _):
        with disk_manager:
            pass

    config_path = (
        "/etc/clickhouse-server/config.d/"
        f"cloud_storage_tmp_disk_object_storage_source_{LINKED_BACKUP_NAME}.xml"
    )
    disks = xmltodict.parse(written[config_path], disable_entities=False)["clickhouse"][
        "storage_configuration"
    ]["disks"]
    assert_equal(
        disks[f"object_storage_source_{LINKED_BACKUP_NAME}"]["endpoint"],
        f"https://minio:9000/backup-bucket/ch_backup/{LINKED_BACKUP_NAME}"
        "/cloud_storage/object_storage/",
    )


def test_restore_downloads_metadata_of_the_backup_holding_data():
    """
    Metadata of a deduplicated part is stored in the backup its data was
    copied into, together with the keys of the objects.
    """
    disk_manager = _make_temporary_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG,
        cloud_storage_disks=["object_storage"],
        data_copied=True,
        source_bucket=None,
        parts=[_make_linked_part()],
    )

    with _capture_config_files():
        with disk_manager:
            pass

    # pylint: disable=protected-access
    download = disk_manager._backup_layout.download_cloud_storage_metadata
    downloaded_backups = [call.args[0].name for call in download.call_args_list]  # type: ignore[attr-defined]
    assert LINKED_BACKUP_NAME in downloaded_backups


@pytest.mark.parametrize(
    "part",
    [
        _make_linked_part(),
        _make_linked_part(name="all_1_1_0_2", link_part_name="all_1_1_0"),
    ],
    ids=["same name", "renamed by a mutation"],
)
def test_restore_copies_a_deduplicated_part_from_its_own_backup(part):
    """
    A part is copied from the temporary disk of the backup holding its data,
    under the name it has there: a mutation renames a part, but its data stays
    stored under the old name.
    """
    disk_manager = _make_temporary_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG,
        cloud_storage_disks=["object_storage"],
        data_copied=True,
        source_bucket=None,
        parts=[part],
    )
    target_disk = Disk(
        "object_storage", "/var/lib/clickhouse/disks/object_storage/", "s3"
    )
    source_disk_name = f"object_storage_source_{LINKED_BACKUP_NAME}"
    # pylint: disable=protected-access
    disk_manager._ch_availible_disks = {
        "object_storage": target_disk,
        source_disk_name: Disk(source_disk_name, BACKUP_DISK_PATH, "s3"),
    }
    table = Table(
        "db1",
        "table1",
        "MergeTree",
        [target_disk],
        [os.path.join(target_disk.path, "store/abc/abcdef")],
        "",
        "",
        None,
    )

    with unittest.mock.patch(
        "ch_backup.clickhouse.disks._ch_disks_copy"
    ) as copy_command:
        disk_manager._run_copy_command(disk_manager._backup_meta, table, part)

    _, from_disk, from_path, _, _, _ = copy_command.call_args.args
    assert_equal(from_disk, source_disk_name)
    assert_equal(from_path, f"shadow/{LINKED_BACKUP_NAME}/store/abc/abcdef/all_1_1_0/")


def test_restore_fails_when_the_backup_holding_data_is_gone():
    """
    A missing backup must be reported instead of silently restoring a table
    without parts of it.
    """
    disk_manager = _make_temporary_disks(
        BACKUP_DISK_CLICKHOUSE_CONFIG,
        cloud_storage_disks=["object_storage"],
        data_copied=True,
        source_bucket=None,
        parts=[_make_linked_part()],
    )
    # pylint: disable=protected-access
    disk_manager._backup_layout.get_backup.side_effect = None  # type: ignore[attr-defined]
    disk_manager._backup_layout.get_backup.return_value = None  # type: ignore[attr-defined]

    with _capture_config_files():
        try:
            with disk_manager:
                pass
            assert False, "Expected ClickHouseDisksException was not raised"
        except ClickHouseDisksException as exc:
            assert LINKED_BACKUP_NAME in str(exc)
