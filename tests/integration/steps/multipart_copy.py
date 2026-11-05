"""Observe multipart requests through the existing test proxy and real S3 storage."""

import gzip
import hashlib
import json
from collections import Counter

import requests
from behave import given, then, when

from tests.integration.modules.ch_backup_cli import CH_BACKUP_CONF_PATH, BackupManager
from tests.integration.modules.clickhouse import ClickhouseClient
from tests.integration.modules.docker import get_container, get_exposed_port
from tests.integration.modules.s3 import S3Client
from tests.integration.modules.steps import get_step_data


@given("a multipart copy backup")
def step_multipart_backup(context):
    client = ClickhouseClient(context, "clickhouse01")
    client.execute("CREATE DATABASE test_db")
    client.execute(
        "CREATE TABLE test_db.big (n UInt64, data UInt64) ENGINE=MergeTree ORDER BY n"
    )
    client.execute(
        "INSERT INTO test_db.big SELECT number, rand64() FROM numbers(3000000) "
        "SETTINGS max_threads=1, max_insert_threads=1"
    )
    client.execute("OPTIMIZE TABLE test_db.big FINAL")
    # S3 listing orders a_probe before big: the probe succeeds on a small object,
    # then the failing multipart transfer runs in an ordinary process-pool worker.
    for table in ("a_probe", "small"):
        client.execute(f"CREATE TABLE test_db.{table} AS test_db.big")
        client.execute(f"INSERT INTO test_db.{table} VALUES (1, 2)")
    manager = BackupManager(context, "clickhouse01")
    manager.backup(name="multipart", databases=["test_db"], data=True)
    source = S3Client(context)
    objects = source.list_objects("ch_backup/multipart/data/test_db/big/")
    assert len(objects) == 1, objects
    context.multipart_key = objects[0]
    context.multipart_size = source.get_object_size(context.multipart_key)
    assert context.multipart_size > 16 * 1024 * 1024, context.multipart_size
    host, port = get_exposed_port(get_container(context, "broken-proxy01"), 4080)
    context.multipart_proxy = f"http://{host}:{port}/__fault__"
    manager.update_config(
        {
            "storage": {
                "proxy_resolver": {
                    "uri": "http://broken-proxy01:4080/__fault__/resolve"
                },
                "retry": {"backup": {"max_attempts": 2, "max_interval": 0}},
            }
        }
    )
    BackupManager(context, "clickhouse02").update_config(
        {"storage": {"credentials": {"bucket": "ch-backup-copy"}}}
    )


@given("multipart copy faults")
def step_multipart_faults(context):
    options = get_step_data(context)
    context.multipart_options = options
    rules = [
        {
            "operation": "UploadPartCopy",
            "part_number": 2,
            "failures": -1 if options.get("permanent") or options.get("rotate") else 1,
            "status": options.get("status", 503),
            "code": (
                "SlowDown" if options.get("status", 503) == 503 else "InternalError"
            ),
        }
    ]
    if options.get("abort_failure"):
        rules.append(
            {
                "operation": "AbortMultipartUpload",
                "failures": -1,
                "status": 403,
                "code": "AccessDenied",
            }
        )
    response = requests.post(
        context.multipart_proxy + "/config",
        json={
            "path": "/ch-backup-copy/" + context.multipart_key,
            "rules": rules,
            "rotate": options.get("rotate", False),
        },
        timeout=10,
    )
    response.raise_for_status()


@when('we copy the multipart backup in "{mode}" mode')
def step_copy_multipart(context, mode):
    if mode == "probe":
        arguments = [
            "python3",
            "-c",
            "import sys; from ch_backup.config import Config; "
            "from ch_backup.storage.engine.s3 import S3StorageEngine, S3RetryPolicy; "
            "c=Config(sys.argv[1]); c['storage']['credentials']['bucket']='ch-backup-copy'; "
            "assert S3StorageEngine(c['storage'], S3RetryPolicy.BACKUP).try_copy_object('ch-backup', sys.argv[2], sys.argv[2])",
            CH_BACKUP_CONF_PATH,
            context.multipart_key,
        ]
    else:
        arguments = [
            "ch-backup",
            "-c",
            CH_BACKUP_CONF_PATH,
            "copy",
            "multipart",
            "--target-config-parameter",
            "storage.credentials.bucket",
            '"ch-backup-copy"',
            "--report-path",
            "/tmp/multipart-report.json",
        ]
        if mode == "keep-going":
            arguments.append("--keep-going")
    context.multipart_mode = mode
    context.command = str(arguments)
    result = get_container(context, "clickhouse01").exec_run(arguments, user="root")
    context.response = result.output.decode()
    context.exit_code = result.exit_code


def _requests(context):
    response = requests.get(context.multipart_proxy + "/state", timeout=10)
    response.raise_for_status()
    return response.json()


@then("only the failed multipart segment was retried")
def step_only_segment_retried(context):
    assert context.exit_code == 0, context.response
    state = _requests(context)
    records = state["requests"]
    operations = Counter(item["operation"] for item in records)
    assert operations["CreateMultipartUpload"] == 1, records
    assert operations["AbortMultipartUpload"] == 0, records
    parts = [item for item in records if item["operation"] == "UploadPartCopy"]
    assert len({item["upload_id"] for item in parts}) == 1, parts
    assert state["resolutions"] >= 2, state
    if not context.multipart_options.get("rotate"):
        assert operations["CompleteMultipartUpload"] == 1, records
        expected_parts = (context.multipart_size + 8 * 1024 * 1024 - 1) // (
            8 * 1024 * 1024
        )
        assert Counter(item["part_number"] for item in parts) == Counter(
            [*range(1, expected_parts + 1), 2]
        ), parts
        repeated = [item for item in parts if item["part_number"] == 2]
        assert len({(item["range"], item["source"]) for item in repeated}) == 1
    source = S3Client(context).download_data(context.multipart_key)
    target = S3Client(context, "ch-backup-copy").download_data(context.multipart_key)
    assert hashlib.sha256(source).digest() == hashlib.sha256(target).digest()


@then("the multipart failure was reported without restarting the object")
def step_multipart_failed(context):
    assert context.exit_code != 0, context.response
    records = _requests(context)["requests"]
    operations = Counter(item["operation"] for item in records)
    assert operations["CreateMultipartUpload"] == 1, records
    assert operations["CompleteMultipartUpload"] == 0, records
    assert operations["AbortMultipartUpload"] == 1, records
    parts = [item for item in records if item["operation"] == "UploadPartCopy"]
    assert len({item["upload_id"] for item in parts}) == 1, records
    assert sum(item["part_number"] == 2 for item in parts) == 2, parts
    assert all(
        count == 1
        for part, count in Counter(item["part_number"] for item in parts).items()
        if part != 2
    ), parts
    assert "SlowDown" in context.response, context.response
    cleanup = (
        "AbortMultipartUpload failed"
        if context.multipart_options.get("abort_failure")
        else "Aborted multipart copy"
    )
    log = context.response
    if context.multipart_mode != "probe":
        result = get_container(context, "clickhouse01").exec_run(
            ["cat", "/var/log/ch-backup/ch-backup.log"], user="root"
        )
        assert result.exit_code == 0, result.output
        log = result.output.decode()
    assert cleanup in log, log
    if context.multipart_mode != "probe":
        assert any(
            "SpawnProcess" in line and cleanup in line for line in log.splitlines()
        ), log
    target = S3Client(context, "ch-backup-copy")
    assert not target.path_exists(context.multipart_key)
    if context.multipart_mode != "probe":
        result = get_container(context, "clickhouse01").exec_run(
            ["cat", "/tmp/multipart-report.json"], user="root"
        )
        assert result.exit_code == 0, result.output
        report = json.loads(result.output)["backups"][0]
        assert report["status"] == "failed" and report["objects_failed"] >= 1, report
        assert (
            sum(
                error.get("source") == context.multipart_key
                and "SlowDown" in error["message"]
                for error in report["errors"]
            )
            == 1
        ), report
        prefix = "ch_backup/multipart/copy_context/"
        for path in target.list_objects(prefix):
            # Confirmations are directly below the fingerprint; manifest, source
            # metadata and failure lists also mention objects that were not copied.
            if path[len(prefix) :].count("/") == 1:
                assert context.multipart_key.encode() not in gzip.decompress(
                    target.download_data(path)
                )
            elif path.endswith("/failed/files.jsonl.gz"):
                assert context.multipart_key.encode() in gzip.decompress(
                    target.download_data(path)
                )
        if context.multipart_mode == "keep-going":
            # Fail-fast can also report interrupted jobs; keep-going must finish
            # independent objects without breaking the process pool.
            assert report["objects_failed"] == 1, report
            assert target.list_objects(
                "ch_backup/multipart/data/test_db/small/"
            ), report


@then("the copied multipart backup restores its data")
def step_multipart_restore(context):
    BackupManager(context, "clickhouse02").restore("multipart")
    query = "SELECT count(), sum(n), sum(data) FROM test_db.big"
    assert ClickhouseClient(context, "clickhouse01").get_response(
        query
    ) == ClickhouseClient(context, "clickhouse02").get_response(query)
