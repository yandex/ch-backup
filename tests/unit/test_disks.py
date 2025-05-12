"""
Unit tests disks module.
"""

import unittest

import xmltodict

from ch_backup.backup_context import BackupContext
from ch_backup.clickhouse.config import ClickhouseConfig
from ch_backup.clickhouse.disks import ClickHouseTemporaryDisks
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

        expected_content = xmltodict.parse(temp_config)
        actual_content = xmltodict.parse(write_result)
        assert_equal(actual_content, expected_content)


def write_collector(x):
    # pylint: disable=global-statement
    global write_result
    write_result += x.decode("utf-8")
