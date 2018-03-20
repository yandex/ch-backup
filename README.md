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
  -c, --config PATH  Configuration file path.
  -h, --help         Show this message and exit.

Commands:
  backup   Perform backup.
  list     List existing backups.
  restore  Restore data from a particular backup.
  show     Show details for a particular backup.
```

## Testing

### Regression

The regression test suite requires:
```
python 3.5
python 3.6
make
tox
docker
```

Once requirements satisfied, the tests can be run by issuing the command:

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

2. Create S3 bucket for backups.
```
$ docker exec -it -u root minio01.test_net_711 mc mb fake-s3/dbaas
```

3. Log in to ClickHouse docker container and you are all set to issue ch-backup
 commands.
```
$ docker exec -it -u root clickhouse01.test_net_711 bash

root@clickhouse01:/# ch-backup backup
20180320T084137

root@clickhouse01:/# ch-backup show LAST
{
    "databases": {},
    "meta": {
        "path": "ch_backup/test_uuid/test_clickhouse/20180320T084137",
        "end_time": "2018-03-20 08:41:37 +0000",
        "rows": 0,
        "start_time": "2018-03-20 08:41:37 +0000",
        "hostname": "clickhouse01.test_net_711",
        "date_fmt": "%Y-%m-%d %H:%M:%S %z",
        "bytes": 0,
        "ch_version": "v1.1.54327-testing",
        "name": "20180320T084137"
    }
}
```

Note: There are no prepopulated data in ClickHouse. So you need to insert some
 data yourself in order to make non-zero backup.
