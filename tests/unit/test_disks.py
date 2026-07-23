"""
Unit tests disks module.
"""

import unittest
from typing import List, Optional

import xmltodict

from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.disks import (
    ClickHouseDisksException,
    ClickHouseTemporaryDisks,
)
from ch_backup.config import DEFAULT_CONFIG, Config
from tests.unit.utils import assert_equal, parametrize

write_result = ""


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
                <history-file>/tmp/.disks-file-history</history-file>
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
                <history-file>/tmp/.disks-file-history</history-file>
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
                <history-file>/tmp/.disks-file-history</history-file>
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
    with unittest.mock.patch(
        "builtins.open",
        new=unittest.mock.mock_open(read_data=clickhouse_config),
        create=True,
    ):
        with unittest.mock.patch("yaml.load", return_value=""):
            context.ch_config = ClickhouseConfig(Config("foo"))
        context.ch_config.load()
    with unittest.mock.patch("builtins.open", new=unittest.mock.mock_open()) as m:
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

        # pylint: disable=global-statement
        global write_result
        write_result = ""
        m().write = write_collector

        # pylint: disable=protected-access
        # Initialise _disks the same way __enter__ does
        disk._disks = (context.ch_config.config.get("storage_configuration") or {}).get(
            "disks"
        ) or {}
        disk._create_temporary_disk(
            context.backup_meta,
            disk_name,
            source["bucket"],
            source["path"],
            source["endpoint"],
        )
        m.assert_called_with(
            f"/etc/clickhouse-server/config.d/cloud_storage_tmp_disk_{disk_name}_source.xml",
            "w",
            encoding="utf-8",
        )

        expected_content = xmltodict.parse(temp_config, disable_entities=False)
        actual_content = xmltodict.parse(write_result, disable_entities=False)
        assert_equal(actual_content, expected_content)


def write_collector(x):
    # pylint: disable=global-statement
    global write_result
    write_result += x.decode("utf-8")


def _make_temporary_disks(
    clickhouse_config_xml: str,
    cloud_storage_disks: Optional[List[str]] = None,
) -> ClickHouseTemporaryDisks:
    """Helper: build ClickHouseTemporaryDisks with mocked dependencies."""
    context = BackupContext(DEFAULT_CONFIG)  # type: ignore[arg-type]
    context.ch_ctl = unittest.mock.MagicMock()
    context.backup_layout = unittest.mock.MagicMock()
    context.backup_meta = unittest.mock.MagicMock()
    context.backup_meta.cloud_storage.disks = cloud_storage_disks or []
    context.backup_meta.cloud_storage.enabled = bool(cloud_storage_disks)
    with unittest.mock.patch(
        "builtins.open",
        new=unittest.mock.mock_open(read_data=clickhouse_config_xml),
        create=True,
    ):
        with unittest.mock.patch("yaml.load", return_value=""):
            context.ch_config = ClickhouseConfig(Config("foo"))
        context.ch_config.load()
    # Pass a dummy bucket when cloud_storage_disks is non-empty, otherwise None
    source_bucket = "test-bucket" if cloud_storage_disks else None
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

    with unittest.mock.patch("builtins.open", new=unittest.mock.mock_open()):
        with disk_manager:
            # pylint: disable=protected-access
            assert disk_manager._disks == {}


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
                "test-bucket",
                "cluster1/shard1/",
                "localhost",
            )
            assert False, "Expected ClickHouseDisksException was not raised"
        except ClickHouseDisksException as exc:
            assert "missing_disk" in str(exc)
            assert "storage_configuration" in str(exc)
