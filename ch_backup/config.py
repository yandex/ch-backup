"""
config module defines Config class and default values
"""

import copy
import socket
from datetime import datetime, timezone
from typing import Any

import yaml
from humanfriendly import parse_size, parse_timespan
from loguru import logger


def _as_seconds(t: str) -> int:
    return int(parse_timespan(t))


def as_datetime(value: Any) -> datetime | None:
    """Parse an optional timezone-aware ISO timestamp and normalize it to UTC."""
    if value is None:
        return None

    error = "Expected an ISO 8601 timestamp with a timezone, or null"
    if isinstance(value, str):
        try:
            # Python 3.10 does not accept the UTC suffix Z in fromisoformat.
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(error) from exc

    # YAML may already have parsed an unquoted timestamp into a datetime.
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(error)
    return value.astimezone(timezone.utc)


DEFAULT_CONFIG: dict[str, Any] = {
    "clickhouse": {
        "data_path": "/var/lib/clickhouse",
        "metadata_path": "/var/lib/clickhouse/metadata",
        "access_control_path": "/var/lib/clickhouse/access",
        "tmp_path": "/var/lib/clickhouse/_mdb_tmp",
        "zk_access_control_path": "/clickhouse/access",
        "config_dir": "/etc/clickhouse-server/config.d/",
        "preprocessed_config_path": "/var/lib/clickhouse/preprocessed_configs/config.xml",
        "named_collections_path": "/var/lib/clickhouse/named_collections",
        "host": socket.gethostname(),
        "protocol": "http",
        "port": None,
        "ca_path": None,
        "connect_timeout": _as_seconds("10 sec"),
        "timeout": _as_seconds("1.5 min"),
        "freeze_timeout": _as_seconds("45 min"),
        "unfreeze_timeout": _as_seconds("1 hour"),
        "restore_replica_timeout": _as_seconds("30 min"),
        "sync_database_replica_timeout": _as_seconds("30 min"),
        "sync_database_replica_poll_interval": 10,
        "sync_database_replica_stall_threshold": 6,
        "drop_replica_timeout": _as_seconds("1 hour"),
        "user": "clickhouse",
        "group": "clickhouse",
        "clickhouse_user": None,
        "clickhouse_password": None,
    },
    "clickhouse_disks": {
        # Supported for ClickHouse 25.9+. None keeps the ClickHouse defaults.
        "s3_retry_attempts": None,
        "s3_retry_initial_delay_ms": None,
        "s3_retry_max_delay_ms": None,
        "s3_slow_all_threads_after_network_error": None,
    },
    "backup": {
        "system_database": "_system",
        "exclude_dbs": [],
        "path_root": None,
        "deduplicate_parts": True,
        "deduplication_start_time": None,
        "deduplication_age_limit": {
            "days": 7,
        },
        "deduplication_batch_size": 500,
        "deduplication_verify_links": True,
        "min_interval": {
            "minutes": 0,
        },
        "time_format": "%Y-%m-%d %H:%M:%S.%f %z",
        # Leave out a data part, or the data of a table, that cannot be backed
        # up, instead of failing the whole backup. Such a backup is created, and
        # list shows how many tables are left without data or some parts.
        "keep_going": False,
        # Retain policy for the purge command that sets the maximum age of
        # backups to keep.
        "retain_time": {},
        # Retain policy for the purge command that sets the maximum number of
        # valid backups to keep.
        "retain_count": None,
        # User data including into backup metadata.
        "labels": {},
        # Backup sources used when none of the source options is passed to the backup
        # command. Empty means all sources. Possible keys (bool):
        #   schema - databases and tables schemas (--schema),
        #   data - tables data, implies schema (--data),
        #   access - access control entities (--access),
        #   udf - user defined functions (--udf),
        #   named_collections - named collections (--nc),
        #   workload_entities - workloads and resources (--workload),
        #   http_handlers - SQL-defined HTTP handlers (--http-handlers).
        # An unknown key selects all sources with a warning.
        "sources": {},
        # Backup of detached parts. Options of the backup command take precedence.
        "detached_parts": {
            "enabled": False,
            "disks": ["object_storage"],
            "reasons": None,
        },
        "keep_freezed_data_on_failure": True,
        "override_replica_name": "{replica}",
        "force_non_replicated": False,
        "backup_access_control": False,
        "restore_context_path": "/tmp/ch_backup_restore_state.json",  # nosec
        "restore_context_sync_threshold_ops": 100,
        "validate_part_after_upload": False,
        "restore_fail_on_attach_error": False,
        "fail_on_unparsable_part_metadata": False,
        "update_metadata_interval": _as_seconds("30 min"),
        "kill_old_freeze_queries": True,
    },
    "copy": {
        "only_with_data": True,
        # Independent target window for local parts; zero preserves source-link reuse only.
        "deduplication_age_limit": {"days": 0},
        # Copy objects on the server side if source and target buckets belong to the same S3 service
        # and are accessible with the same credentials.
        "server_side_copy": True,
        "resume": True,
        # Copying of data stored on object storage (S3) disks.
        "cloud_storage": {
            # Location of the source disk objects. If not set, it's taken from backup metadata
            # (for backups that were already copied) or from ClickHouse disk configuration.
            # A stored endpoint must match a configured storage/disk endpoint unless explicitly overridden.
            "source": {
                "endpoint_url": None,
                "bucket": None,
                "path": None,
                "access_key_id": None,
                "secret_access_key": None,
            },
            # Location where disk objects are copied to. Objects of the backup are put
            # under "<path>/<backup name>/". Endpoint and credentials default to ones of the target storage.
            "target": {
                "endpoint_url": None,
                "bucket": None,
                "path": "",
                "access_key_id": None,
                "secret_access_key": None,
            },
        },
    },
    "restore": {
        "use_inplace_cloud_restore": False,
        # Max table name length. Actually it is just a result of the getMaxTableNameLengthForDatabase(<atomic_db>) ch function.
        "max_table_name": 206,
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
            # HTTP connect and read timeout, in seconds (not a total DNS/request deadline).
            "timeout": 10,
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
        # Base S3 retry policy; 1 attempt disables retries.
        "retry_max_attempts": 10,
        # Base upper bound of the randomized exponential pause, in seconds.
        "retry_max_interval": 60,
        # Command-specific fields below override the base values above.
        "retry": {
            # All S3 requests made by the list command, including metadata reads.
            "list": {
                "max_attempts": 5,
                "max_interval": 10,
            },
            # All S3 requests made while creating or copying a backup.
            "backup": {
                "max_attempts": 60,
            },
            # All S3 requests made while restoring a backup.
            "restore": {
                "max_attempts": 60,
            },
        },
        # Enable bulk delete (DeleteObjects in S3 API)
        "bulk_delete_enabled": True,
        # How many files we can delete by bulk delete operation in one call
        "bulk_delete_chunk_size": 1000,
        # The number of uploading threads for multipart storage uploading
        "uploading_threads": 4,
        # The maximum number of objects the stage's input queue can hold simultaneously, `0`is unbounded
        "queue_size": 10,
    },
    "rate_limiter": {
        # Upper bound of network loading per second on the uploading stage for each worker. (bytes/sec)
        # Example: for following conf:
        #          workers: 4
        #          max_upload_rate: 100
        #          Total upload rate = 400 bytes/sec.
        # If the value is 0, then the traffic rate is unlimited.
        "max_upload_rate": 0,
        # The wait time for the next attempt upload the chunk to the storage. The value in seconds.
        "retry_interval": 0.01,
    },
    # Same structure as 'storage' section, but for cloud storage
    "cloud_storage": {
        # Encryption and compression are applied to cloud storage metadata only.
        # Data copied by "copy_data" is stored as ClickHouse wrote it.
        "encryption": False,
        "compression": True,
        "copy_data": False,
    },
    "compression": {
        "type": "gzip",
        # The maximum number of objects the stage's input queue can hold simultaneously, `0` is unbounded
        "queue_size": 10,
    },
    "encryption": {
        "enabled": True,
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
        # The number of processes for parts restoring from S3 disks.
        "cloud_storage_restore_workers": 4,
        # The number of threads for copying data of S3 disks into the backup.
        "cloud_storage_backup_workers": 4,
        # The number of threads for parallel freeze of tables
        "freeze_threads": 4,
        # To execute freeze efficiently, we should parallelize freeze operations. We have two options where we can parallelize:
        # 1) Inside ch-backup: We can perform several `ALTER TABLE FREEZE PARTITION` queries. Then freeze_partition_threads are used.
        # 2) Inside clickhouse(preferable): Since 25.11 clickhouse can parallelize `ALTER TABLE FREEZE` queries. Then freeze_table_query_max_threads are used.
        #    https://github.com/ClickHouse/ClickHouse/pull/71743
        "parallelize_freeze_in_clickhouse": True,
        # The number of threads for parallel freeze of partitions. If set to 0, will freeze table in one query.
        "freeze_partition_threads": 16,
        "freeze_table_query_max_threads": 16,
        # The number of threads for parallel drop replica
        "drop_replica_threads": 8,
    },
    "pipeline": {
        # Is asynchronous pipelines used (based on Pypeln library)
        "async": True,
    },
    "main": {
        "user": "clickhouse",
        "group": "clickhouse",
        "working_dir": "/tmp",
        "drop_privileges": True,
        "ca_bundle": [],
        "disable_ssl_warnings": False,
        "tracemalloc": 0,
    },
    "loguru": {
        "logs_directory": "/var/log/ch-backup",
        "formatters": {
            "ch-backup": "{time:YYYY-MM-DD HH:mm:ss,SSS} {process.name:11} {process.id:5} [{level:8}] {extra[logger_name]}: {message}",
            "clickhouse-disks": "{time:YYYY-MM-DD HH:mm:ss,SSS} {extra[tag]} {process.id:5} [{level:8}]: {message}",
        },
        "handlers": {
            "ch-backup": {
                "sink": "ch-backup.log",
                "level": "DEBUG",
                "format": "ch-backup",
            },
            "zookeeper": {
                "sink": "ch-backup.log",
                "level": "DEBUG",
                "format": "ch-backup",
            },
            "botocore": {
                "sink": "boto.log",
                "format": "ch-backup",
                "filter": {
                    "botocore": "INFO",
                    "botocore.endpoint": "DEBUG",
                    "botocore.vendored.requests": "DEBUG",
                    "botocore.parsers": "DEBUG",
                },
            },
            "clickhouse-disks": {
                "sink": "clickhouse-disks.log",
                "level": "DEBUG",
                "format": "clickhouse-disks",
            },
            "urllib3.connectionpool": {
                "sink": "boto.log",
                "level": "DEBUG",
                "format": "ch-backup",
            },
        },
    },
    "zookeeper": {
        "secure": False,
        "cert": None,
        "key": None,
        "ca": None,
        "timeout": 10,
        "connect_timeout": 10,
        "hosts": [],
        "root_path": "",
        "user": None,
        "password": None,
        "randomize_hosts": True,
    },
    "lock": {
        "flock": False,
        "zk_flock": False,
        "flock_path": "/tmp/flock.lock",
        "zk_flock_path": "/tmp/zk_flock.lock",
        "exitcode": 0,
        "lock_timeout": _as_seconds("1 min"),
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

    def derive(
        self, config_file: str | None = None, patch: dict | None = None
    ) -> "Config":
        """
        Return a copy of the config with applied changes from config file and the patch.
        """
        config = copy.deepcopy(self)
        if config_file:
            config.merge(self._load_config_file(config_file))
        if patch:
            config.merge(patch)
        return config

    def _read_config(self, file_name):
        custom_config = self._load_config_file(file_name)
        if custom_config:
            self._recursively_update(self._conf, custom_config)

    @staticmethod
    def _load_config_file(file_name: str) -> dict:
        with open(file_name, "r", encoding="utf-8") as fileobj:
            try:
                return yaml.safe_load(fileobj) or {}
            except yaml.YAMLError as e:
                raise RuntimeError(f"Failed to load config file: {e}")

    def __getitem__(self, item):
        try:
            return self._conf[item]
        except KeyError:
            logger.critical('Config item "{}" was not defined', item)
            raise

    def __setitem__(self, item, value):
        try:
            self._conf[item] = value
        except KeyError:
            logger.critical('Config item "{}" was not defined', item)
            raise

    def get(self, key: str, default: Any = None) -> Any:
        """
        Returns value by key or default
        """

        return self._conf.get(key, default)
