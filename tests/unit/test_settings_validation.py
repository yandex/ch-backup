from unittest import mock

import pytest

from ch_backup.clickhouse.control import ClickhouseCTL
from ch_backup.exceptions import ConfigurationError


def _ctl_config(settings=None):
    config = {
        "data_path": "/tmp",
        "timeout": 10,
        "freeze_timeout": 10,
        "unfreeze_timeout": 10,
        "restore_replica_timeout": 10,
        "drop_replica_timeout": 10,
    }
    if settings is not None:
        config["settings"] = settings
    return config


@mock.patch("ch_backup.clickhouse.control.ClickhouseClient")
@mock.patch.object(ClickhouseCTL, "get_disks", return_value={})
def _new_ctl(settings, _get_disks_mock, client_cls_mock):
    client = client_cls_mock.return_value
    client.query.return_value = "25.10.2.65"
    client.settings = mock.Mock()
    ClickhouseCTL(_ctl_config(settings=settings), {}, {})


@pytest.mark.parametrize(
    "settings",
    [
        ["a", "b"],
        "not-a-dict",
        42,
    ],
)
def test_settings_non_mapping_raises_configuration_error(settings):
    with pytest.raises(ConfigurationError):
        _new_ctl(settings)
