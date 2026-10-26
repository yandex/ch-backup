[![license](https://img.shields.io/github/license/yandex/ch-backup)](https://github.com/yandex/ch-backup/blob/main/LICENSE)
[![tests status](https://img.shields.io/github/actions/workflow/status/yandex/ch-backup/.github%2Fworkflows%2Fworkflow.yml?event=push&label=tests&logo=github)](https://github.com/yandex/ch-backup/actions/workflows/workflow.yml?query=event%3Apush)
[![chat](https://img.shields.io/badge/telegram-chat-blue)](https://t.me/+O4gURpLnQ604OTE6)
[![Python](https://img.shields.io/badge/Python-%3E%3D3.10-3776AB?logo=python&logoColor=white)](https://github.com/yandex/ch-backup/blob/main/pyproject.toml#L26)
# ch-backup

Backup tool for ClickHouse DBMS.

It allows to perform backups to S3 compatible storage and restore from backup
 data in the case of original data corruption.

Backup is performed for tables of MergeTree engine family only as these are
 the only tables that support consistent data snapshots without server shutdown.

The tool also supports deduplication at part-level granularity. It's set up
 through configuration file and enabled by default. You could also
 set a start time to exclude earlier backups from deduplication.


## Restore after part download errors

Restore attaches a part only after all its files have been downloaded and unpacked,
or its cloud-storage copy has completed. Failed parts are recorded as `invalid`
with the original download or copy error in the restore state file.
With `--keep-going`, other parts continue restoring; without it, the command
stops with the download or copy error. With `--keep-going`, set
`backup.restore_fail_on_attach_error: true` to return a nonzero exit code if any
part failed to download, copy, or attach. The default remains `false`.
A new restore invocation retries invalid parts, attaches already downloaded
parts, and skips restored parts.
Successfully downloaded parts are attached in their original part order,
regardless of download completion order. Saved failure details survive restores
of other tables; a part's error is removed after that part is successfully attached.
Only failures from the current invocation affect its exit code.

## Restore without cloud storage

`ch-backup restore --skip-cloud-storage BACKUP` restores local parts and skips
cloud parts, including detached parts. It also adapts MergeTree schemas and
materialized views with their own storage to the target server:

- A policy missing from `system.storage_policies` is replaced with `default`.
- An explicit named `disk` setting is removed, even if that disk exists on the
  target. The table uses the target server's default MergeTree storage policy
  (including a `replicated_merge_tree` override), which must contain a local disk.
  Dynamic `disk(...)` expressions and target engine defaults specifying `disk`
  instead of a policy are not supported by this adaptation.
  If removing `disk` would require replacing an existing table (including a
  detached table or a table with the same UUID), restore stops before deleting
  any database or table, even with `--keep-going`. Cloud parts would not be
  restored after that deletion. Use an empty destination, or explicitly remove
  the existing table after deciding that its data can be discarded.
- TTL MOVE rules referencing disks or volumes absent from the resulting policy
  are removed as whole rules. DELETE, conditional DELETE, RECOMPRESS, GROUP BY,
  column TTLs and available MOVE rules are preserved.
- Local parts whose original disk is outside the adapted policy go to its first
  local disk, in volume priority and disk order.

For replicated tables, use the same flag and storage configuration on every
target replica.
Restore each replicated shard's data on one replica. On its other replicas, use
`--schema-only --skip-cloud-storage`, optionally with `--parts-from-backup BACKUP`
to accelerate recovery. The existing `--parts-from-backup` mode downloads eligible
parts into `detached/`, verifies their file checksums, then runs
`SYSTEM RESTORE REPLICA`; replication fetches any remaining parts. This mode
requires restore without ordinary data loading, so `--schema-only` is still
needed. Restore non-replicated tables separately with their data.
If adapting a replicated table's TTL conflicts with existing Keeper metadata,
restore fails before dropping local databases or tables or cleaning replica metadata, including
with `--keep-going` and any `--clean-zookeeper-mode`. It never resets shared Keeper
metadata automatically. Restore to a separate Keeper table path or resolve the
conflict before retrying. A per-table Keeper lock covers the check and table
creation. Direct ZooKeeper access, using the same root as ClickHouse, is required
only when the replicated table TTL is changed; unchanged schemas and policy-only
changes retain the ordinary restore requirements.

Removed disk settings, policy replacements and removed TTL rules are logged. With `--keep-going`,
independent tables continue after a schema adaptation or creation failure, but
the command reports those failures with a nonzero exit status. A shared Keeper
metadata conflict stops the restore immediately. The behavior without
`--skip-cloud-storage` is unchanged.

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
  copy     Copy backup to another storage (another S3 bucket, endpoint or path).
  delete   Delete particular backup.
  list     List existing backups.
  purge    Purge outdated backups.
  restore  Restore data from a particular backup.
  show     Show details for a particular backup.
```

### Copying backups to another storage

`ch-backup copy` copies a backup (or all backups with `--all`) to another S3 bucket,
endpoint or `backup.path_root`. The main configuration describes the source storage. The
target storage is described by the main configuration merged with a configuration file passed
in `--target-config` and values passed in `--target-config-parameter`, so it's enough to
specify only differing settings:

```bash
# Copy the last backup to another bucket of the same S3 service.
ch-backup copy LAST --target-config-parameter storage.credentials.bucket '"backups-dr"'

# Copy all backups to another S3 service.
ch-backup copy --all --target-config /etc/yandex/ch-backup/dr.conf

# Continue after errors and save confirmed results, including on failure.
ch-backup copy --all --keep-going --target-config /etc/yandex/ch-backup/dr.conf \
    --report-path /tmp/ch-backup-copy-report.json

# Copy files of the backup on the server side first, then stream data of object storage disks,
# possibly from another host without access to the source storage.
ch-backup copy LAST --scope files --server-side-copy --target-config /etc/yandex/ch-backup/dr.conf
ch-backup copy LAST --scope cloud_storage --keep-going --no-server-side-copy \
    --target-config /etc/yandex/ch-backup/dr.conf
```

```yaml
# /etc/yandex/ch-backup/dr.conf
storage:
    credentials:
        endpoint_url: https://s3.other.example
        access_key_id: ...
        secret_access_key: ...
        bucket: backups-dr
```

Behavior and limitations:

- A final text report lists copied, skipped, failed and unprocessed backups. `--report-path`
  also saves a JSON report (schema version 1) once the command finishes, including failures
  and handled termination signals. Any failure gives a nonzero exit code.
- `--keep-going` continues copying independent objects and subsequent backups after errors.
  Local planning errors are collected per part or metadata file; available independent objects
  are still copied and the incomplete backup remains failed. Each plan's destinations are validated
  before any writes. Unreadable target backups reserve their names and do not block independent
  backups; unusable deduplication links are materialized from the source when possible.
  Shared configuration, listing, lock and executor failures still stop the run.
  Object counters include rewritten metadata tarballs and count only confirmed completions.
  `known_bytes_copied` sums source sizes of successfully copied objects, excluding rewritten
  tarballs whose final stored sizes are not known. Operations without a confirmed result are
  not counted as successes. Storage credentials are not included in the report.
  Error details spill to temporary files above 64 KiB per backup and JSON output is streamed,
  so large failure reports do not accumulate entirely in RAM. Copy tasks are drained in batches
  of at most 256 objects; the validated object plan and backup metadata still take memory
  proportional to their size.
- New backups store `backup_struct.json` and `backup_light_struct.json` as plaintext JSON,
  with `meta.metadata_encrypted=false`. This does not change `meta.encrypted`, which describes
  encryption of data and schema. DDL, access control, named collections and UDF definitions
  retain their encryption. The public JSON exposes names, paths, sizes, checksums, deduplication
  links, labels and failure messages; do not put secrets in labels.
- Backup data is copied as is, without decryption. New backups can be copied with
  `encryption.type=nacl` and no `encryption.key` configured on either side. Encryption settings
  (`encryption.type`, `encryption.chunk_size`, and keys when provided) and `compression.type`
  must match between the copy configs. Restore still needs the original encryption key.
  Reading and restoring old backups remains supported, including encrypted full JSON and
  S3 metadata archives. Missing encryption flags keep their historical defaults of `true`.
  Legacy encrypted metadata, including metadata of deduplication dependencies, still requires
  its original key when it must be read. Local deduplicated parts can be materialized from
  the file list in the selected backup without reading the owner's metadata.
  To start an independent series of keyless-copyable backups,
  set `backup.deduplication_start_time` to the beginning of the series when enabling the new format.
- If both buckets belong to the same S3 service and are accessible with the same credentials, objects
  are copied on the server side. Otherwise, data is streamed through ch-backup. Server-side copy can be
  disabled with `--no-server-side-copy`.
- The copy becomes valid (gets the state `created`) only after all its data is copied. If copying is
  interrupted, the copy remains in the state `creating` or `failed`, and the next run of the command
  resumes it. Confirmed copies are recorded in the copy context, a journal kept in the target storage
  under `<backup>/copy_context/`. It is saved when 10000 confirmations accumulate or the first
  confirmation arrives 60 seconds after the previous save, and when copying fails or is interrupted by
  a signal. The next run skips an object only if the context records it with the
  same source, target and size and the target storage still holds it with that size: size alone does not
  establish that the contents are correct, and a failed or partial delete may have removed recorded
  objects. Other objects are overwritten. If the target listing or a context file cannot be read, the
  affected objects are copied again. `objects_skipped` in the report counts skipped objects. Before
  copying, resuming holds the listing of the target backup in memory, comparable to the object plan.
  `--no-resume` (or `copy.resume: false`) ignores records of previous runs and overwrites all objects;
  the run still records confirmed copies, so an interrupted run should be repeated with `--no-resume`
  to overwrite every object.
  The context is not copied with the backup and is deleted together with it.
  Completed backups are skipped.
  When a linked part has already been materialized in a successfully copied backup, subsequent
  backups reuse that copy, including mutation-renamed parts and full cloud-storage backups.
- Copying consists of two scopes: `files` copies files of the backup, `cloud_storage` copies data
  and metadata of object storage disks kept in their buckets, rewriting the metadata to reference
  the copied data. Backups without such data, including ones created with
  `--copy-cloud-storage-data`, are copied entirely by `files`, and their scope `cloud_storage` is
  completed right away. `--scope` (can be given multiple times) copies only the specified scopes,
  all of them by default. Each run chooses its own copy mode with `--[no-]server-side-copy`. `files`
  also copies the original disk metadata into
  `<backup>/copy_context/<id>/source/` and saves the list of its objects in
  `<backup>/copy_context/<id>/manifest/files.jsonl.gz` (locations and sizes, without credentials). A
  run without `files` does not access the source storage: it takes copies from the target storage
  (the specified ones, or with `--all` every copy whose `cloud_storage` is not completed, or the
  newest of them for `LAST`), reads the copy's full metadata and disk metadata from `source/`,
  copies disk metadata that needs no rewriting from there and checks `files` by the manifest.
  The copy retains the original cloud storage location until it is published as `created`. So `files` must be copied first, from a
  host with access to the source storage; `cloud_storage` needs only access to the buckets of the
  disks. A scope that is not copied is planned only if it is completed, to check it with the same
  settings as copying it, so copying only the scope `files` needs no settings of object storage data
  while `cloud_storage` is not completed. Statuses of the scopes (`pending`, `completed`, `failed`)
  are saved in `meta.copy` of the copy, for example `state: creating, copy: {scopes: {files:
  completed, cloud_storage: pending}, updated_at: ...}`, where `updated_at` is when copying was last
  saved. The copy becomes `created` once every scope is completed. Until then the report shows the
  backup as `partial` with `backup_state: creating`, and the command exits with 0 if the copied
  scopes succeed. Saved statuses are not trusted: a scope that is not copied is completed only if
  the copy context and the target listing confirm all its objects, otherwise it becomes `pending`
  again, e.g. after a failed delete. `--no-resume` overwrites objects of the scopes that are copied
  and still checks the other scopes this way. Between scopes, the copy is in the state `creating`.
  `purge` and `delete --purge-partial` on the target storage keep a copy in the state `creating` or
  `failed` that was updated within `backup.retain_incomplete_copy_time` (7 days by default, empty to
  disable) and treat older ones as any other incomplete backup; older versions of ch-backup do not
  know this rule. With `--keep-going`, objects of a scope that failed to copy are listed in
  `<backup>/copy_context/<id>/failed/<scope>.jsonl.gz` (source and target keys, size and error) and
  the scope becomes `failed`; the next run of the scope copies only unconfirmed objects. The list is
  rewritten by each run of the scope in which objects fail and removed once the scope completes.
  Disk metadata that cannot be planned is not copied, so it never overwrites rewritten disk
  metadata; a later run copies it. Copy scopes of one backup one after another: the local lock
  serializes runs on one host, runs on different hosts require `lock.zk_flock`.
- Copy acquires the configured source and target locks through completion, including retries and
  failure cleanup. To coordinate copy with delete and purge across processes, enable `lock.zk_flock`
  and use the same ZooKeeper ensemble and lock path for every operation on the respective storage.
  Locking remains optional; the local file lock retains its shared semantics.
  Without this coordination, do not run copy concurrently with delete or purge on the same storage.
- By default, deduplicated data parts of the backup are stored in older backups. If such a backup is already
  present in the target storage, the deduplication link is kept. A link to a local part is also kept
  if the copy of that backup is not completed but already holds files of the part, e.g. after its
  scope `files`, as deduplication of new backups does. Otherwise, the data part is copied
  into the copied backup. `--all` copies older backups first, so deduplication is kept in the target storage.
- Set `copy.deduplication_age_limit: {days: 90}` in the target configuration to enable an
  independent deduplication window for active parts on local disks. The default `{days: 0}`
  disables this additional lookup; a zero duration keeps the existing source-link behavior.
  The value accepts the same units as a Python `timedelta` (weeks through microseconds),
  with finite nonnegative numbers. Target command-line configuration overrides the target file,
  which overrides the main configuration, following the usual recursive merge rules.
  When enabled, copy matches parts by database, table, name (allowing mutation suffix changes),
  checksum, disk, size, file list, layout and encryption, even if source stores the part anew
  or links to a different owner. The newest matching physical owner with available files is used.
  Its original backup `start_time` must be within the period before the start of the copy command;
  both bounds are inclusive and fixed before resolving `LAST` or waiting for locks.
  Owners without a timestamp or with a future timestamp are skipped. Copying preserves the
  original timestamp. A recent reference does not extend an old owner's window.
  This additional lookup reuses only TAR parts, whose stored archive size can be checked.
  Legacy per-file parts are copied from source because their metadata has no individual file sizes.
  This applies to source links too: without an eligible target owner, the part is copied from source.
  The window is independent of `backup.deduplication_age_limit`, `deduplication_start_time`
  and `deduplicate_parts`; historical backups copied with `--all` use the same current-time bound.
  Reading full metadata and listing part files in eligible target backups costs additional S3
  requests. Each candidate's full metadata is read once for the index, file listings are grouped
  by table, and index memory grows with the number of physical local parts in the window.
  The index retains a fixed-size fingerprint of each file list instead of its file names.
  Loading a candidate still requires memory proportional to that backup's full metadata.
  Existing completed copies are skipped and existing duplicates are not compacted.
  Cloud-storage and detached parts retain their existing behavior. Restore uses the existing
  link format; delete and purge preserve referenced parts in partially deleted owners until
  the references are removed, even when an owner is outside the deduplication window.
- Full cloud-storage backups created with `--copy-cloud-storage-data` keep their data in the backup
  bucket. Copying them moves that data into the target backup bucket, including linked parts when
  their owning backup is absent there. No ClickHouse disk credentials or `--target-cloud-storage-bucket`
  are needed; restore and deletion use the normal full-backup behavior.
- Backups without `--copy-cloud-storage-data` reference objects in the bucket of the S3 disk. These
  objects are copied to the bucket set with `--target-cloud-storage-bucket` under
  `<--target-cloud-storage-path>/<backup name>/`, so the copy doesn't depend on the source cluster.
  The location of the source objects is taken from ClickHouse configuration of the disk (so the command
  should be run on a host of the source cluster), or it can be set with `--cloud-storage-source-bucket` and
  `--cloud-storage-source-path`. Credentials can be overridden in the `copy.cloud_storage` config section.
  Disk metadata is unencrypted by default (`cloud_storage.encryption=false`); the saved flag
  continues to control reading of older encrypted archives. Before uploading an unencrypted
  archive, ch-backup rejects malformed or unsupported disk metadata, unexpected trailing content
  and nonempty inline file data.
  Each file is limited to 1 MiB, checked and archived from the same bounded memory buffer.
  Larger disk metadata files also require `cloud_storage.encryption=true`.
  Such files require `cloud_storage.encryption=true`, and copying that backup requires a key.
  The flag only controls disk metadata: blobs retain the encryption applied by ClickHouse or S3.
  To use a separate worker, provide source endpoint, bucket, path and credentials explicitly;
  neither a local ClickHouse configuration nor a connection to the source server is required.
  Without the original disk configuration, leave the source path unset for V5 metadata to
  copy its full object keys as stored; set the source prefix for older relative-key metadata.
  Source endpoint, bucket and path overrides are applied independently: omitted fields retain the
  values from stored source metadata or disk configuration. An explicitly empty source path selects
  the bucket root.
  A source endpoint from backup metadata must match the configured backup-storage or ClickHouse-disk
  endpoint (ignoring trailing slashes). To copy from another endpoint, set
  `copy.cloud_storage.source.endpoint_url` explicitly in the target configuration.
  Otherwise, copy fails before making requests to the stored endpoint.
  For version-5 metadata containing full object keys, a source path override replaces the original
  prefix from stored source metadata or disk configuration. Copy fails before writing if that prefix
  is unknown or a key is outside it. Without a path override, full keys are read as stored.
  Restore of the copy uses the copied objects by default; they must be accessible with credentials of the
  object storage disk of the host where the backup is restored.
  Copies reuse unchanged whole parts from completed backups in the same target cloud bucket and path.
  Copy compares archived object references because ClickHouse can change metadata checksums on each freeze.
  Each copied part stores a fingerprint derived from the original object namespace and references.
  Later copies preserve that identity when objects move to another destination.
  Older destination copies without this fingerprint remain readable but cannot supply deduplication owners.
  Backup metadata records each part's owning backup. Restore reads each part from its owner's location.
  Copying a backup without its owner at the destination materializes the linked part in the new copy.
  Deleting a copy retains external objects by default. Add `--delete-cloud-storage-data` to
  `delete` or `purge` to remove copied objects that surviving backups do not reference. Original backup data is preserved.
  Cleanup selects objects from saved disk metadata and uses backup-storage credentials by default. Configure
  `copy.cloud_storage.target` for separate credentials or a different endpoint.
  Metadata must be trusted and readable. Encrypted disk metadata requires its original key.
  Part links and persisted source fingerprints also trust backup metadata. Fingerprints identify data but do not authenticate metadata.
  Partial cleanup removes unreferenced parts and their archived disk metadata. Parts referenced by
  surviving backups retain both their objects and disk metadata. Objects shared with retained parts also remain.
  Batch cleanup validates all selected backups before deletion. Failed cleanup can be retried.
  In-memory archives have a 1 GiB limit, adjustable in bytes with `filesystem.max_in_memory_tarball_size`.
  Restore accepts stored endpoints only when their HTTP(S) origin matches a configured disk or backup-storage endpoint.
  Use `--cloud-storage-source-endpoint` to explicitly authorize another endpoint for restore.
  Restore preserves the HTTP/HTTPS scheme of the stored endpoint. Its `--cloud-storage-source-endpoint`
  option accepts a full HTTP(S) URL or `host[:port]`; the latter uses the original disk's scheme.
- ClickHouse 26.8+ SQL-defined HTTP handlers are included in full backups; use `--http-handlers` with backup
  or restore handlers only.
  Export requires `SHOW HANDLERS`, `SELECT ... system.handlers`, and a server setting
  `display_secrets_in_show_and_select=1` plus grant `displaySecretsInShowAndSelect` if you have any secrets.
  In the latter case, any `[HIDDEN]`, including literals, is rejected.
  Restore requires `CREATE HANDLER` and `DROP HANDLER` for replacements.
  Identical definitions are skipped; changed definitions use non-atomic `DROP/CREATE`.
  Concurrent handler DDL is unsupported. Stop other changes to the selected handlers during restore.
- Workload and resource restore skips identical definitions and uses `CREATE OR REPLACE`
  for changes, preserving dependencies. Entities absent from the backup remain; XML-defined
  target entities are skipped. Parents omitted from the backup must already exist. Replacing
  shared parents or resources can affect other entities. Type conflicts and server restrictions
  abort restore; concurrent workload/resource DDL is unsupported.

### Backup targets in configuration

What the `backup` command saves can be set in the configuration file instead of command
options, so the command line does not need to change when the set of targets does:

```yaml
backup:
  sources:            # empty (default) means all sources
    schema: true
    data: true        # implies schema
    access: true
    udf: true
    named_collections: true
    workload_entities: true
  detached_parts:
    enabled: true
    disks: [object_storage]
    reasons: null     # all reasons
```

Command options take precedence per group. Any of `--schema`, `--data`, `--access`, `--udf`,
`--nc`, `--workload` or `--schema-only` replaces `backup.sources` entirely.
`--detached-parts` enables detached parts regardless of the configuration, and
`--detached-parts-disks` and `--detached-parts-reasons` replace the configured lists.
An unknown key in `backup.sources` (a typo, or a source added in a newer version) makes a full
backup with a warning, so a configuration written for newer versions still works with older
ones and never misses data. Values must be booleans: a quoted `"false"` fails the backup.
`--config-parameter` merges dictionaries key by key, so to drop configured sources for one
run, pass `--config-parameter backup.sources null`. The `restore` command does not use these
settings.

### Detached parts

Detached MergeTree parts can be included explicitly with `--detached-parts`.
The existing `--databases` and `--tables` options select their tables; optional
`--detached-parts-disks` and `--detached-parts-reasons` comma-separated filters
select disks and detach reasons. The disk list defaults to `object_storage`;
with no reason filter, every detached part on the selected disks is copied.
Use an empty reason argument for parts detached by a user:

```bash
ch-backup backup --detached-parts --tables db.table \
    --detached-parts-disks object_storage,archive_storage \
    --detached-parts-reasons ''
```

The feature remains disabled unless `--detached-parts` is passed or
`backup.detached_parts.enabled` is set in the configuration. Disk names select their actual table directories; the value reported
by `system.detached_parts` is not used, so cache-disk naming differences
between ClickHouse versions do not affect selection.

Detached parts are scanned and uploaded on a best-effort basis without a
point-in-time snapshot. A part created during the backup may be omitted; a part
that disappears or becomes inaccessible during scanning or upload is skipped
with a warning. Only successfully uploaded detached parts are added to backup
metadata, and an individual detached-part failure does not fail the regular
table backup. Executor failures and termination signals still abort the backup.

Restore is also opt-in: pass `--restore-detached-parts` to a data restore.
Without this option detached-part metadata is ignored. Enabled restores put
the parts under the corresponding table's `detached` directory on their
original disk and never attach them automatically. Schema-only restore leaves
them out. If the destination name is occupied by different data, the existing
directory is preserved and the part is restored with the first free
ClickHouse-style `_tryN` suffix. A directory with matching files, size and
checksum is treated as already restored. If the backup has no checksum,
equality cannot be proved and `_tryN` is used.

```bash
ch-backup restore --restore-detached-parts BACKUP
```

For object-storage disks, the archive contains the local part metadata files,
not the referenced bucket objects, and its recorded size is the aligned size
of those metadata files. `--copy-cloud-storage-data` still copies active table
parts, but emits a warning and keeps detached parts metadata-only. Such backup
archives are copied normally by `ch-backup copy`. Restoring an object-storage
detached part only writes those metadata files into `detached`; it does not run
`clickhouse-disks copy`, copy blobs, or rewrite their object keys. The restored
metadata therefore continues to refer to the same objects as the source part,
which must be accessible through the destination disk configuration. This also
applies after `ch-backup copy`; `--cloud-storage-source-*` options do not alter
detached-part metadata. `--skip-cloud-storage` skips both active and detached
object-storage parts.

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
 ClickHouse and SeaweedFS (open source S3-compatible storage server) docker
 containers.

1. Create and run docker containers.
```
$ make start-test-env
...
Creating seaweedfs01.test_net_711 ...
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

The scheduler starts features with more selected scenarios first and gives free
workers the next eligible feature. Features tagged `@parallel_heavy` reserve two
slots (one when `INTEGRATION_JOBS=1`); `@parallel_exclusive` reserves every slot
and waits for the active features to finish. The initial heavy features are
`backup_restore` and `freeze_parallel`.

After a failure, no new features start; active features finish. `--stop` also
remains enabled within each feature. Failed steps print their feature, scenario
and step in the coordinator log before collecting diagnostics. GitHub Actions
also receives an error annotation. Failed processes print the log tail.
Interrupted or incomplete runs fail, even when JUnit output is missing.
Normal completion, errors and handled termination
signals clean up only owned containers, networks and image tags. Workspaces and
reports remain available for diagnosis. Cleanup failures are reported as failures;
the runner never performs a global Docker prune.

Results are printed at startup under `staging/parallel/<run-id>/results/`:

- `summary.json`: final status, selected feature outcomes, reserved slots, image
  IDs and actual ClickHouse versions. Features not started after a failure have
  status `not_run`, distinct from version skips.
- Per-feature directories: full `behave.log` and standard Behave `junit/` reports,
  including skipped scenarios. Failure-only stage diagnostics are written to
  `stage-failures.jsonl`; worker logs and container diagnostics are retained for
  failed runs. Exit codes, report completeness and stage failures determine the
  result; unfinished scenarios cannot turn a run green.

All twelve existing CI combinations use three slots. JUnit and diagnostics are
uploaded on both success and failure with run-attempt-specific artifact names.
