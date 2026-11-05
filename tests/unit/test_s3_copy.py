"""Managed copy request budgets, using the real transfer executor."""

from collections import Counter
from copy import deepcopy
from unittest.mock import Mock, patch

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.storage.engine.s3 import S3StorageEngine
from ch_backup.storage.engine.s3.s3_copy_client import S3MultipartCopyError


@pytest.mark.parametrize("max_attempts", [1, 2])
@pytest.mark.parametrize(
    ("failed_operation", "status"),
    [
        ("UploadPartCopy", 200),
        ("UploadPartCopy", 403),
        ("UploadPartCopy", 503),
        ("CompleteMultipartUpload", 403),
        ("CompleteMultipartUpload", 503),
    ],
)
def test_copy_budget_is_per_request(
    max_attempts: int, status: int, failed_operation: str
) -> None:
    config = deepcopy(DEFAULT_CONFIG["storage"])
    config["credentials"].update(
        bucket="target", access_key_id="test", secret_access_key="test"
    )
    config["retry_max_attempts"] = max_attempts
    engine = S3StorageEngine(config)
    getattr(engine, "_retry").sleep_function = Mock()
    client = boto3.client(
        "s3",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    setattr(
        engine, "_s3_client_factory", Mock(create_s3_client=Mock(return_value=client))
    )
    calls: Counter[str] = Counter()

    def request(operation, parameters):
        calls[operation] += 1
        if operation == "HeadObject":
            return {"ContentLength": 24 * 1024 * 1024}
        if operation == "CreateMultipartUpload":
            return {"UploadId": "upload-id"}
        if operation == failed_operation and parameters.get("PartNumber", 2) == 2:
            if status == 200:
                # A malformed success response fails in s3transfer, outside the API adapter.
                return {}
            raise ClientError(
                {
                    "Error": {"Code": "AccessDenied" if status == 403 else "SlowDown"},
                    "ResponseMetadata": {"HTTPStatusCode": status},
                },
                operation,
            )
        if operation == "UploadPartCopy":
            return {"CopyPartResult": {"ETag": str(parameters["PartNumber"])}}
        assert operation == "AbortMultipartUpload", operation
        return {}

    with (
        patch.object(client, "_make_api_call", side_effect=request) as api,
        pytest.raises(S3MultipartCopyError, match="upload-id"),
    ):
        engine.copy_object("source", "object", "object")

    failed_parts = [
        call
        for call in api.call_args_list
        if call.args[0] == failed_operation and call.args[1].get("PartNumber", 2) == 2
    ]
    assert len(failed_parts) == (max_attempts if status == 503 else 1)
    assert calls["CreateMultipartUpload"] == 1
    assert calls["AbortMultipartUpload"] == 1
    assert calls["CompleteMultipartUpload"] == (
        0 if failed_operation == "UploadPartCopy" else len(failed_parts)
    )
    assert all(call.args == failed_parts[0].args for call in failed_parts)
