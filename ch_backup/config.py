"""
config module defines Config class and default values
"""

import copy
import socket
from typing import Any

import yaml
from humanfriendly import parse_size, parse_timespan

from ch_backup import logging


def _as_seconds(t: str) -> int:
    return int(parse_timespan(t))


DEFAULT_CONFIG = {
    "clickhouse": {
        "data_path": "/var/lib/clickhouse",
        "metadata_path": "/var/lib/clickhouse/metadata",
        "access_control_path": "/var/lib/clickhouse/access",
        "zk_access_control_path": "/clickhouse/access",
        "config_dir": "/etc/clickhouse-server/config.d/",
        "preprocessed_config_path": "/var/lib/clickhouse/preprocessed_configs/config.xml",
        "host": socket.gethostname(),
        "protocol": "http",
        "port": None,
        "ca_path": None,
        "connect_timeout": _as_seconds("10 sec"),
        "timeout": _as_seconds("1.5 min"),
        "freeze_timeout": _as_seconds("45 min"),
        "system_unfreeze_timeout": _as_seconds("1 hour"),
        "restart_disk_timeout": _as_seconds("8 hours"),
        "user": "clickhouse",
        "group": "clickhouse",
        "clickhouse_user": None,
        "clickhouse_password": None,
    },
    "backup": {
        "exclude_dbs": [],
        "path_root": None,
        "deduplicate_parts": True,
        "deduplication_age_limit": {
            "days": 7,
        },
        "min_interval": {
            "minutes": 0,
        },
        "time_format": "%Y-%m-%d %H:%M:%S.%f %z",
        # Retain policy for the purge command that sets the maximum age of
        # backups to keep.
        "retain_time": {},
        # Retain policy for the purge command that sets the maximum number of
        # valid backups to keep.
        "retain_count": None,
        # User data including into backup metadata.
        "labels": {},
        "keep_freezed_data_on_failure": True,
        "override_replica_name": "{replica}",
        "force_non_replicated": False,
        "backup_access_control": False,
        "restore_context_path": "/tmp/ch_backup_restore_state.json",  # nosec
        "validate_part_after_upload": False,
        "restore_fail_on_attach_error": False,
    },
    "storage": {
        "type": "s3",
        "credentials": {
            "endpoint_url": None,
            "access_key_id": None,
            "secret_access_key": None,
            "bucket": None,
        },
        "boto_config": {
            "addressing_style": "auto",
            "region_name": "us-east-1",
        },
        # Service that provides proxy connection settings at runtime. For example, it can be used
        # to facilitate direct access to S3 servers bypassing balancer.
        "proxy_resolver": {
            "uri": None,
            "proxy_port": None,
        },
        "disable_ssl_warnings": True,
        # Chunk size used when uploading / downloading data, in bytes.
        "chunk_size": parse_size("8 MiB"),
        # Buffer size, in bytes.
        "buffer_size": parse_size("32 MiB"),
        # The maximum number of chunks on which uploading or downloading data
        # can be split. If data_size > chunk_size * max_chunk_count,
        # chunk_size will be multiplied on a required number of times
        # to satisfy the limit.
        "max_chunk_count": 10000,
        # Enable bulk detete (DeleteObjects in S3 API)
        "bulk_delete_enabled": True,
        # How many files we can delete by bulk delete operation in one call
        "bulk_delete_chunk_size": 1000,
        # The number of uploading threads for multipart storage uploading
        "uploading_threads": 4,
        # The maximum number of objects the stage's input queue can hold simultaneously, `0`is unbounded
        "queue_size": 10,
    },
    # Same structure as 'storage' section, but for cloud storage
    "cloud_storage": {
        "encryption": True,
    },
    "encryption": {
        "type": "nacl",
        # Chunk size used when encrypting / decrypting data, in bytes.
        "chunk_size": parse_size("8 MiB"),
        # Buffer size, in bytes.
        "buffer_size": parse_size("32 MiB"),
        # Encryption key.
        "key": None,
        # The maximum number of objects the stage's input queue can hold simultaneously, `0` is unbounded
        "queue_size": 10,
    },
    "filesystem": {
        "type": "unlimited",
        # Chunk size used when reading from / writing to filesystem, in bytes.
        "chunk_size": parse_size("1 MiB"),
        # Buffer size, in bytes.
        "buffer_size": parse_size("32 MiB"),
        # The maximum number of objects the stage's input queue can hold simultaneously, `0`is unbounded
        "queue_size": 50,
    },
    "multiprocessing": {
        # The number of processes allocating for data processing. If set to 0, all processing will be performed
        # in the main process.
        "workers": 4,
    },
    "pipeline": {
        # Is asynchronous pipelines used (based on Pypeln library)
        "async": True,
    },
    "main": {
        "user": "clickhouse",
        "group": "clickhouse",
        "drop_privileges": True,
        "ca_bundle": [],
        "disable_ssl_warnings": False,
    },
    "logging": {
        "version": 1,
        "formatters": {
            "ch-backup": {
                "format": "%(asctime)s %(processName)-11s %(process)-5d [%(levelname)-8s] %(name)s: %(message)s",
            },
            "boto": {
                "format": "%(asctime)s %(processName)-11s %(process)-5d [%(levelname)-8s] %(name)s: %(message)s",
            },
        },
        "handlers": {
            "ch-backup": {
                "class": "logging.FileHandler",
                "filename": "/var/log/ch-backup/ch-backup.log",
                "formatter": "ch-backup",
            },
            "boto": {
                "class": "logging.FileHandler",
                "filename": "/var/log/ch-backup/boto.log",
                "formatter": "boto",
            },
            "clickhouse-disks": {
                "class": "logging.FileHandler",
                "filename": "/var/log/ch-backup/clickhouse-disks.log",
                "formatter": "ch-backup",
            },
        },
        "loggers": {
            "ch-backup": {
                "handlers": ["ch-backup"],
                "level": "DEBUG",
            },
            "botocore": {
                "handlers": ["boto"],
                "level": "INFO",
            },
            "botocore.endpoint": {
                "level": "DEBUG",
            },
            "botocore.vendored.requests": {
                "level": "DEBUG",
            },
            "botocore.parsers": {
                "level": "DEBUG",
            },
            "urllib3.connectionpool": {
                "handlers": ["boto"],
                "level": "DEBUG",
            },
            "clickhouse-disks": {
                "handlers": ["clickhouse-disks"],
                "level": "INFO",
            },
            "zookeeper": {
                "handlers": ["ch-backup"],
                "level": "DEBUG",
            },
        },
    },
    "zookeeper": {
        "secure": False,
        "cert": None,
        "key": None,
        "ca": None,
        "connect_timeout": 10,
        "hosts": [],
        "root_path": "",
        "user": None,
        "password": None,
    },
    "lock": {
        "flock": False,
        "zk_flock": False,
        "flock_path": "/tmp/flock.lock",
        "zk_flock_path": "/tmp/zk_flock.lock",
        "exitcode": 0,
    },
}


class Config:
    """
    Config for all components
    """

    def __init__(self, config_file: str) -> None:
        self._conf = copy.deepcopy(DEFAULT_CONFIG)
        self._read_config(file_name=config_file)

    def _recursively_update(self, base_dict, update_dict):
        for key, value in update_dict.items():
            if isinstance(value, dict):
                if key not in base_dict:
                    base_dict[key] = {}
                self._recursively_update(base_dict[key], update_dict[key])
            else:
                base_dict[key] = value

    def merge(self, patch_dict):
        """
        Merge config with the patch.
        """
        self._recursively_update(self._conf, update_dict=patch_dict)
        return self._conf

    def _read_config(self, file_name):
        with open(file_name, "r", encoding="utf-8") as fileobj:
            try:
                custom_config = yaml.safe_load(fileobj)
                if custom_config:
                    self._recursively_update(self._conf, custom_config)
            except yaml.YAMLError as e:
                raise RuntimeError(f"Failed to load config file: {e}")

    def __getitem__(self, item):
        try:
            return self._conf[item]
        except KeyError:
            logging.critical('Config item "%s" was not defined', item)
            raise

    def __setitem__(self, item, value):
        try:
            self._conf[item] = value
        except KeyError:
            logging.critical('Config item "%s" was not defined', item)
            raise

    def get(self, key: str, default: Any = None) -> Any:
        """
        Returns value by key or default
        """

        return self._conf.get(key, default)
