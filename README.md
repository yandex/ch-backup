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

The regression test suite requires:
```
python 3.6
make
tox 3
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
        "date_fmt": "%Y-%m-%d %H:%M:%S %z",
        "rows": 0,
        "bytes": 0,
        "hostname": "clickhouse01.test_net_711",
        "ch_version": "v1.1.54327-testing"
    }
}
```

Note: There are no prepopulated data in ClickHouse. So you need to insert some
 data yourself in order to make non-zero backup.
