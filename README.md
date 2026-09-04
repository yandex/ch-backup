[![license](https://img.shields.io/github/license/yandex/ch-backup)](https://github.com/yandex/ch-backup/blob/main/LICENSE)
[![tests status](https://img.shields.io/github/actions/workflow/status/yandex/ch-backup/.github%2Fworkflows%2Fworkflow.yml?event=push&label=tests&logo=github)](https://github.com/yandex/ch-backup/actions/workflows/workflow.yml?query=event%3Apush)
[![chat](https://img.shields.io/badge/telegram-chat-blue)](https://t.me/+O4gURpLnQ604OTE6)

# ch-backup

Backup tool for ClickHouse DBMS.

It allows to perform backups to S3 compatible storage and restore from backup
 data in the case of original data corruption.

Backup is performed for tables of MergeTree engine family only as these are
 the only tables that support consistent data snapshots without server shutdown.

The tool also supports deduplication at part-level granularity. It's set up
 through configuration file and enabled by default.


## Installation

In order to get an up-to-date version of ch-backup, run `make build`. It will produce
a Python wheel (.whl) package that can be installed using `pip install` or `uv tool install`.

<details>
<summary>Example</summary>

```
$ make build
uv build
Building source distribution...
Building wheel from source distribution...
Successfully built dist/ch_backup-2.690.221827381.tar.gz
Successfully built dist/ch_backup-2.690.221827381-py3-none-any.whl
```

```
$ uv tool install dist/*whl
Resolved 29 packages in 434ms
Prepared 1 package in 18ms
Installed 29 packages in 44ms
 + boto3==1.35.99
 + botocore==1.35.99
 + certifi==2026.2.25
 + cffi==2.0.0
 + ch-backup==2.690.221827381 (from file:///Users/alex-burmak/workspace/ch-backup/dist/ch_backup-2.690.221827381-py3-none-any.whl)
 + charset-normalizer==3.4.7
 + click==8.1.8
 + cloup==3.0.9
 + humanfriendly==10.0
 + idna==3.11
 + jmespath==1.1.0
 + kazoo==2.11.0
 + loguru==0.7.3
 + packaging==26.0
 + psutil==7.2.2
 + pycparser==3.0
 + pynacl==1.6.2
 + pypeln==0.4.9
 + python-dateutil==2.9.0.post0
 + pyyaml==6.0.3
 + requests==2.33.1
 + s3transfer==0.10.4
 + setuptools==80.10.2
 + six==1.17.0
 + stopit==1.1.2
 + tabulate==0.10.0
 + tenacity==9.1.4
 + urllib3==2.6.3
 + xmltodict==1.0.4
Installed 1 executable: ch-backup
```
</details>


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

The regression test suite contains run of static code analysis tools (isort, black, codespell, ruff, pylint, mypy),
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
$ make start-test-env
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
export CLICKHOUSE_VERSION=25.4.5.24
make all
```

### Unit tests

Unit tests are implemented based on [pytest](https://docs.pytest.org/en/latest/) testing framework.

The tests can be run as a part of regression test suite with `make all` or
separately with `make test-unit`. Additionally, `PYTEST_ARGS` parameter
can be used to pass additional arguments to underlying `py.test` invocation.
For example, `make test-unit PYTES_ARGS='-k dedup'` executes only deduplication-realted tests.

### Integration tests

Integration tests verify ch-backup functionality in isolated virtual environment.
[Docker](https://docs.docker.com/) is used as a virtualization technology and
[Behave](https://behave.readthedocs.io/en/stable/) as a testing framework.

The tests can be run as a part of regression test suite with `make all` or
separately with `make test-integration`. Additionally, `BEHAVE_ARGS` parameter
can be used to pass additional arguments to underlying `behave` invocation.
For example, `make test-integration BEHAVE_ARGS='-i ssl_support'` executes
tests that belongs to SSL support feature (`ssl_support.feature`).

#### Parallel integration tests on one Docker host

```bash
make test-integration-parallel INTEGRATION_JOBS=3
make test-integration-parallel INTEGRATION_JOBS=1 BEHAVE_ARGS='-i ssl_support'
uv run python -m tests.integration.parallel --jobs 3 --dry-run
```

Each worker uses its own source snapshot, session file, configuration, Docker
network and containers. Dependencies and the wheel are prepared once; worker
images are built sequentially using the shared Docker build cache. Tests install
the wheel just as in the serial runner. The original checkout's session and
containers are not reused. Do not run `make clean-test-env` while a parallel run
is active: it removes the parent `staging/` directory.

Only `tests/integration/ch_backup.featureset` controls suite membership. Adding a
feature there is sufficient; there are no worker-specific feature lists. The
optional `INTEGRATION_FEATURESET` selects a different list of files under `tests/`.
`BEHAVE_ARGS` supports the usual feature, scenario-name and tag filters. Feature
files are indivisible, including `@dependent-scenarios` features: their scenarios
retain their original order and environment hooks.

The scheduler starts longer features first and gives free workers the next
eligible feature. Features tagged `@parallel_heavy` reserve two slots (one when
`INTEGRATION_JOBS=1`); `@parallel_exclusive` reserves every slot and waits for the
active features to finish. The initial heavy features are `backup_restore` and
`freeze_parallel`; this is a conservative classification from their fixtures,
not a measured claim about runner utilization.

After a failure, no new features start; active features finish. `--stop` also
remains enabled within each feature. Failed steps print their feature, scenario,
step and traceback in the coordinator log before collecting diagnostics. GitHub
Actions also receives an error annotation. Setup failures and crashes without
step results print the process log tail. Interrupted or incomplete runs fail, even
when JUnit output is missing. Normal completion, errors and handled termination
signals clean up only owned containers, networks and image tags. Workspaces and
reports remain available for diagnosis. Cleanup failures are reported as failures;
the runner never performs a global Docker prune.

Results are printed at startup under `staging/parallel/<run-id>/results/`:

- `summary.json`: final status, selected feature outcomes, process wall times,
  worker preparation times, image IDs and actual ClickHouse versions. Features
  not started after a failure have status `not_run`, distinct from version skips.
- Per-feature directories: `behave.log`, `junit/`, `outcome.json`, `stages.jsonl`
  and `resources.jsonl`. Failure diagnostics are also retained in the worker's
  `staging/logs/` directory and copied into the feature's reports.
- Global `resources.jsonl`: five-second samples of host CPU, memory, swap, disk
  space and I/O, coordinator descendants' CPU/RSS/I/O, and owned containers' CPU,
  memory and block I/O.
  Per-feature samples retain only that worker's containers and process tree.
- `resource-summary.json` and `resource-summary.md`: per-feature CPU cores
  (mean/p95), simultaneous container working set plus Python RSS, I/O rates,
  restart counts and wall times, neighbors, and host pressure during each feature.
  The Markdown table is also published in GitHub Job Summary. Missing counters
  are reported as `n/a`, never zero. Five-second samples miss short bursts and
  processes that start and exit between samples; RSS may include shared pages.

Use the resource summary to choose features for a four-worker benchmark. Compare
full runs on the same pinned images; p95 demand alone cannot guarantee a safe
combination. Keep `@parallel_heavy` for two-slot features. Use the existing
`@parallel_exclusive` only after a repeatable failure with neighbors and resource
pressure, followed by a successful isolated comparison; no additional tag is
needed. Host disk I/O includes other work and cannot be attributed to one feature.
Regenerate a summary from downloaded artifacts without Docker:

```bash
uv run python -m tests.integration.resource_summary /path/to/results
```

JUnit durations exclude environment hooks in Behave. Use measured process wall
times, which include feature setup and teardown, for scheduling and comparisons:

```bash
make test-integration-parallel INTEGRATION_JOBS=3 \
  INTEGRATION_TIMINGS=staging/parallel/<previous-run-id>/results/summary.json
```

Without timings, expanded scenario counts provide the initial weights. Unknown
features use the mean measured seconds per scenario when available. Failed and
skipped feature timings are not reused. Use a matching Python/ClickHouse version
and the same filters when reusing timings.

#### Benchmark and CI activation

The manual **integration benchmark** workflow compares the original serial runner
and the new runner with one, two, three and four slots on `ubuntu-22.04`, Python 3.10.
Each mode runs twice on separate runners. Supply an exact ClickHouse version
resolved from `latest` (for example `26.8.2.7`) so comparisons do not mix releases.
The existing manual ClickHouse-version workflow is unchanged.

Benchmark reports include total `make` wall time, including wheel and image
preparation. The parallel runner's own summary starts after the wheel is built;
use the benchmark's `benchmark.json` and Actions job duration for end-to-end
comparisons. A clean checkout can run one benchmark locally:

```bash
CLICKHOUSE_VERSION=26.8.2.7 uv run python -m tests.integration.benchmark --mode 3
```

Compare full-suite outcomes, version skips, both repetitions' wall times, peak
memory, swap activity and available disk space. If three slots are unstable or
slower than two, use two. Mark a feature exclusive only after confirming that it
passes alone and suffers resource-related failures when sharing the runner.
Do not hide contention by increasing timeouts or automatically retrying failures.

All twelve existing CI combinations run with three slots without changing job
names. Adjust `INTEGRATION_JOBS` in the main workflow after comparing CI results:
use `2` for two slots or `1` to restore the original serial runner.
JUnit and diagnostics are uploaded on both success and failure, with run-attempt
specific artifact names. Local timings on a larger machine do not establish the
speedup or memory requirements of the 4-vCPU, 16-GB CI runner.
