"""Direct SDK configuration, inheritance and validation before storage access."""

from copy import deepcopy
from unittest.mock import Mock, patch

import pytest
from boto3.s3.transfer import TransferConfig

from ch_backup.ch_backup import ClickhouseBackup
from ch_backup.config import DEFAULT_CONFIG, Config
from ch_backup.exceptions import ConfigurationError
from ch_backup.storage.engine.s3.s3_client_factory import S3ClientFactory
from ch_backup.storage.engine.s3.s3_config import (
    build_botocore_config,
    build_transfer_config,
    validate_s3_module_config,
)
from ch_backup.storage.engine.s3.s3_engine import S3StorageEngine
from ch_backup.storage.object_copy import make_storage_config


def _storage(**modules):
    storage = deepcopy(DEFAULT_CONFIG["storage"])
    storage["credentials"].update(
        access_key_id="test",
        secret_access_key="test",
        bucket="bucket",
        endpoint_url="http://127.0.0.1:1",
    )
    storage["module_config"] = modules
    return storage


def test_defaults_and_legacy_config():
    storage = _storage()
    storage.pop("module_config")  # Callers can still pass old storage dictionaries.
    storage["boto_config"] = {"region_name": "eu-west-1", "addressing_style": "path"}
    config = vars(build_botocore_config(storage))
    assert config["region_name"] == "eu-west-1"
    assert config["s3"]["addressing_style"] == "path"
    assert config["retries"] == {"max_attempts": 0}
    assert build_transfer_config(storage).__dict__ == TransferConfig().__dict__


@pytest.mark.parametrize(
    "retries",
    [{}, {"mode": "standard"}, {"max_attempts": 2}, {"total_max_attempts": 3}],
)
def test_explicit_retries_replace_default(retries):
    assert (
        vars(build_botocore_config(_storage(botocore={"retries": retries})))["retries"]
        == retries
    )


def test_client_options_and_input_are_preserved():
    storage = _storage(
        botocore={
            "connect_timeout": 0.5,
            "read_timeout": None,
            "max_pool_connections": 32,
            "region_name": "eu-west-2",
            "s3": {"payload_signing_enabled": False},
            "user_agent_extra": "module-config-test",
            "retries": {"max_attempts": 2},
        }
    )
    storage["boto_config"]["addressing_style"] = "path"
    original = deepcopy(storage)
    factory = S3ClientFactory(storage)
    for _ in range(2):
        client = factory.create_s3_client()
        config = client.meta.config
        assert config.connect_timeout == 0.5
        assert config.read_timeout is None
        assert config.max_pool_connections == 32
        assert config.region_name == "eu-west-2"
        assert config.s3["addressing_style"] == "path"
        assert config.s3["payload_signing_enabled"] is False
        assert config.user_agent_extra == "module-config-test"
        assert config.retries["total_max_attempts"] == 3
        client.close()
    assert storage == original


@pytest.mark.parametrize(
    "proxies", [None, {}, {"https": "http://user:password@proxy:8080"}]
)
def test_explicit_proxies_bypass_resolver(proxies):
    storage = _storage(botocore={"proxies": proxies})
    storage["proxy_resolver"]["uri"] = "http://resolver"
    with patch.object(S3ClientFactory, "_resolve_proxies") as resolve:
        client = S3ClientFactory(storage).create_s3_client()
        assert client.meta.config.proxies == proxies
        client.close()
        resolve.assert_not_called()


def test_resolver_is_refreshed_without_explicit_proxies():
    factory = S3ClientFactory(_storage(botocore={"read_timeout": 5}))
    with patch.object(
        factory, "_resolve_proxies", side_effect=[{"http": "first"}, {"http": "second"}]
    ):
        for proxy in ("first", "second"):
            client = factory.create_s3_client()
            assert client.meta.config.proxies == {"http": proxy}
            assert client.meta.config.read_timeout == 5
            client.close()


@pytest.mark.parametrize(
    "modules",
    [
        None,
        [],
        {1: {}},
        {"unknown": {}},
        {"botocore": None},
        {"transfer": False},
        {"botocore": {1: 2}},
        {"transfer": {"unknown": 1}},
        {"botocore": {"unknown": 1}},
        *[
            {"botocore": {name: value}}
            for name, value in (
                ("connect_timeout", "10"),
                ("read_timeout", 0),
                ("read_timeout", float("inf")),
                ("max_pool_connections", False),
                ("max_pool_connections", 1.5),
                ("retries", []),
                ("retries", {"mode": "secret-invalid-mode"}),
                ("retries", {"total_max_attempts": 0}),
                ("retries", {"max_attempts": 1.5}),
                ("retries", {"total_max_attempts": True}),
                ("s3", {"addressing_style": "bad"}),
            )
        ],
        *[
            {"transfer": {name: value}}
            for name, value in (
                ("max_concurrency", 1.5),
                ("max_concurrency", True),
                ("max_concurrency", None),
                ("multipart_threshold", 0),
                ("multipart_chunksize", "5 MiB"),
                ("max_io_queue", float("nan")),
                ("use_threads", "false"),
                ("max_bandwidth", -1),
            )
        ],
    ],
)
def test_invalid_module_options(modules):
    storage = _storage()
    storage["module_config"] = modules
    with pytest.raises(ConfigurationError, match="storage.module_config") as exc:
        validate_s3_module_config(storage)
    assert "secret-invalid-mode" not in str(exc.value)
    assert exc.value.__cause__ is None


def test_managed_transfer_options(tmp_path):
    options = {
        "multipart_threshold": 1024,
        "multipart_chunksize": 5 * 1024**2,
        "max_concurrency": 2,
        "use_threads": False,
        "max_bandwidth": None,
    }
    engine = S3StorageEngine(_storage(transfer=options))
    client = Mock()

    # Exercise every managed entrypoint, retaining real file handling.
    def download(_bucket, _key, target, **_kwargs):
        target.write(b"data")

    client.download_fileobj.side_effect = download
    setattr(
        engine, "_s3_client_factory", Mock(create_s3_client=Mock(return_value=client))
    )
    source = tmp_path / "source"
    source.write_bytes(b"data")
    assert engine.upload_file(str(source), "/uploaded") == "uploaded"
    engine.download_file("/downloaded", str(tmp_path / "target"))
    assert engine.download_data("/data") == b"data"
    engine.copy_object("source-bucket", "/source", "/copy")
    for call in client.method_calls:
        config = call.kwargs["Config"]
        assert isinstance(config, TransferConfig)
        assert all(getattr(config, name) == value for name, value in options.items())


def test_config_inheritance_and_cloud_storage(tmp_path):
    main, target = tmp_path / "main.yaml", tmp_path / "target.yaml"
    main.write_text(
        "storage:\n  module_config:\n    botocore:\n      retries: {mode: standard, total_max_attempts: 3}\n    transfer: {max_concurrency: 2}\n"
    )
    target.write_text(
        "storage:\n  module_config:\n    botocore:\n      retries: {total_max_attempts: 5}\n"
    )
    config = Config(str(main))
    derived = config.derive(
        str(target),
        {"storage": {"module_config": {"transfer": {"max_concurrency": 4}}}},
    )
    assert derived["storage"]["module_config"] == {
        "botocore": {"retries": {"mode": "standard", "total_max_attempts": 5}},
        "transfer": {"max_concurrency": 4},
    }
    cloud = make_storage_config(derived["storage"], "http://disk", "cloud", "id", "key")
    assert cloud["module_config"] == derived["storage"]["module_config"]
    cloud["module_config"]["transfer"]["max_concurrency"] = 8
    assert derived["storage"]["module_config"]["transfer"]["max_concurrency"] == 4
    assert config["storage"]["module_config"]["transfer"]["max_concurrency"] == 2


def test_rejected_before_context_locks_or_network(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("")
    config = Config(str(path))
    backup = ClickhouseBackup(config)
    invalid = config.derive(
        patch={"storage": {"module_config": {"transfer": {"max_concurrency": 0}}}}
    )
    with (
        patch("ch_backup.ch_backup.BackupContext") as context,
        patch("ch_backup.ch_backup.BackupLayout") as layout,
        patch("ch_backup.ch_backup.storage_locks") as locks,
        patch(
            "ch_backup.storage.engine.s3.s3_client_factory.boto3.session.Session"
        ) as session,
    ):
        for operation in (
            lambda: ClickhouseBackup(invalid),
            lambda: backup.reload_config(invalid),
            lambda: backup.copy(invalid, ["new"]),
        ):
            with pytest.raises(
                ConfigurationError,
                match="storage.module_config.transfer.max_concurrency",
            ):
                operation()
        for mock in (context, layout, locks, session):
            mock.assert_not_called()
    assert backup.config is config
