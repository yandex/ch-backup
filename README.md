[![license](https://img.shields.io/github/license/yandex/ch-backup)](https://github.com/yandex/ch-backup/blob/main/LICENSE)
[![tests status](https://img.shields.io/github/actions/workflow/status/yandex/ch-backup/.github%2Fworkflows%2Fworkflow.yml?event=push&label=tests)](https://github.com/yandex/ch-backup/actions/workflows/workflow.yml?query=event%3Apush)

# ch-backup

Backup tool for ClickHouse DBMS.

It allows to perform backups to S3 compatible storage and restore from backup
 data in the case of original data corruption.

Backup is performed for tables of MergeTree engine family only as these are
 the only tables that support consistent data snapshots without server shutdown.

The tool also supports deduplication at part-level granularity. It's set up
 through configuration file and enabled by default.


## Usage

```bash
Usage: ch-backup [OPTIONS] COMMAND [ARGS]...

  Tool for managing ClickHouse backups.

Options:
  -c, --config PATH        Configuration file path.
  --protocol [http|https]  Protocol used to connect to ClickHouse server.
  --port INTEGER           Port used to connect to ClickHouse server.
  --ca-path TEXT           Path to custom CA bundle path for https protocol.
  --insecure               Disable certificate verification for https
                           protocol.
  -h, --help               Show this message and exit.

Commands:
  backup   Perform backup.
  delete   Delete particular backup.
  list     List existing backups.
  purge    Purge outdated backups.
  restore  Restore data from a particular backup.
  show     Show details for a particular backup.
```

## Testing

### Regression

The regression test suite contains run of static code analysis tools (isort, black, flake8, pylint, mypy, bandit),
unit tests and integration tests.

The tests can be run by issuing the command:

```bash
make all
```

### Manual

The following steps describe how to set up testing infrastructure on top of
 ClickHouse and Minio (open source S3-compatible storage server) docker
 containers.

1. Create and run docker containers.
```
$ make start_env
...
Creating minio01.test_net_711 ...
Creating clickhouse01.test_net_711 ...
Creating clickhouse02.test_net_711 ... done
```

2. Log in to ClickHouse docker container and you are all set to issue ch-backup
 commands.
```
$ docker exec -it -u root clickhouse01.test_net_711 bash

root@clickhouse01:/# ch-backup backup
20180320T084137

root@clickhouse01:/# ch-backup show LAST
{
    "databases": {},
    "meta": {
        "name": "20180320T084137",
        "path": "ch_backup/20180320T084137",
        "start_time": "2018-03-20 08:41:37 +0000",
        "end_time": "2018-03-20 08:41:37 +0000",
        "time_format": "%Y-%m-%d %H:%M:%S %z",
        "rows": 0,
        "bytes": 0,
        "hostname": "clickhouse01.test_net_711",
        "ch_version": "v1.1.54327-testing"
    }
}
```

Note: There are no prepopulated data in ClickHouse. So you need to insert some
 data yourself in order to make non-zero backup.

### Testing new versions

```
export CLICKHOUSE_VERSION=21.11.1.8636
make all
```

### Unit tests

Unit tests are implemented based on [pytest](https://docs.pytest.org/en/latest/) testing framework.

The tests can be run as a part of regression test suite with `make all` or
separately with `make unit_test`. Additionally, `PYTEST_ARGS` parameter
can be used to pass additional arguments to underlying `py.test` invocation.
For example, `make unit_tests PYTES_ARGS='-k dedup'` executes only deduplication-realted tests.

### Integration tests

Integration tests verify ch-backup functionality in isolated virtual environment.
[Docker](https://docs.docker.com/) is used as a virtualization technology and
[Behave](https://behave.readthedocs.io/en/stable/) as a testing framework.

The tests can be run as a part of regression test suite with `make all` or
separately with `make integration_test`. Additionally, `BEHAVE_ARGS` parameter
can be used to pass additional arguments to underlying `behave` invocation.
For example, `make integration_test BEHAVE_ARGS='-i ssl_support'` executes
tests that belongs to SSL support feature (`ssl_support.feature`).
