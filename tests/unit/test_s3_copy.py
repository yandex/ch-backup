"""Exercise copy retries through the SDK parser and managed multipart transfer."""

from collections import Counter
from copy import deepcopy
from unittest.mock import patch

import pytest
from botocore.awsrequest import AWSResponse
from botocore.exceptions import ClientError, ReadTimeoutError

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.storage.engine.s3.s3_copy_client import S3MultipartCopyError
from ch_backup.storage.engine.s3.s3_engine import S3StorageEngine


def _engine(attempts: int = 2) -> S3StorageEngine:
    config = deepcopy(DEFAULT_CONFIG["storage"])
    config["credentials"].update(
        bucket="target", access_key_id="test", secret_access_key="test"
    )
    config.update(retry_max_attempts=attempts, retry_max_interval=0)
    return S3StorageEngine(config)


class _RawResponse:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def stream(self, **_kwargs):
        yield self.body


@pytest.mark.parametrize("code", ["InternalError", "AccessDenied"])
@pytest.mark.parametrize(
    "operation, parameters, root",
    [
        ("copy_object", {"CopySource": "source/key"}, "CopyObjectResult"),
        (
            "upload_part_copy",
            {"CopySource": "source/key", "UploadId": "upload", "PartNumber": 1},
            "CopyPartResult",
        ),
        (
            "complete_multipart_upload",
            {"UploadId": "upload", "MultipartUpload": {"Parts": []}},
            "CompleteMultipartUploadResult",
        ),
    ],
)
def test_copy_http_200_error(
    operation: str, parameters: dict, root: str, code: str
) -> None:
    bodies = [
        f"<Error><Code>{code}</Code><Message>injected error</Message></Error>",
        f"<{root}><ETag>etag</ETag></{root}>",
    ]
    responses = [
        AWSResponse("https://s3.example", 200, {}, _RawResponse(body.encode()))
        for body in bodies
    ]
    request = getattr(_engine(), "_copy_request")
    # Replace only the transport: use real XML parsing, signing and retry hooks,
    # including when a failed request causes the factory to replace the client.
    with patch(
        "botocore.httpsession.URLLib3Session.send", side_effect=responses
    ) as send:
        if code == "AccessDenied":
            with pytest.raises(ClientError) as exc:
                request(operation, Bucket="target", Key="key", **parameters)
            assert exc.value.response["Error"]["Code"] == code
            assert send.call_count == 1
        else:
            result = request(operation, Bucket="target", Key="key", **parameters)
            assert result.get("ETag", result.get(root, {}).get("ETag")) == "etag"
            assert send.call_count == 2


@pytest.mark.parametrize(
    "destination, attempts",
    [
        ("ours", 1),
        ("ours", 2),
        ("stale", 2),
        ("wrong-size", 2),
        ("missing", 2),
        ("denied", 2),
    ],
)
def test_lost_complete_response(destination: str, attempts: int) -> None:
    calls: Counter = Counter()
    metadata: dict = {}
    size = 9 * 1024 * 1024

    def request(operation: str, parameters: dict) -> dict:
        calls[operation] += 1
        if operation == "HeadObject":
            if parameters["Bucket"] == "source":
                return {"ContentLength": size}
            if destination in ("missing", "denied"):
                raise ClientError(
                    {
                        "Error": {
                            "Code": (
                                "NotFound"
                                if destination == "missing"
                                else "AccessDenied"
                            )
                        }
                    },
                    operation,
                )
            return {
                "ContentLength": size - (destination == "wrong-size"),
                "Metadata": (
                    {
                        key.title() if attempts == 2 else key: value
                        for key, value in metadata.items()
                    }
                    if destination != "stale"
                    else {"ch-backup-copy-id": "old-copy"}
                ),
                "ETag": "completed-etag",
            }
        if operation == "CreateMultipartUpload":
            metadata.update(parameters.get("Metadata", {}))
            return {"UploadId": "upload"}
        assert parameters["UploadId"] == "upload"
        if operation == "UploadPartCopy":
            return {"CopyPartResult": {"ETag": f"part-{parameters['PartNumber']}"}}
        if operation == "CompleteMultipartUpload":
            assert parameters["MultipartUpload"]["Parts"] == [
                {"ETag": "part-1", "PartNumber": 1},
                {"ETag": "part-2", "PartNumber": 2},
            ]
            if calls[operation] == 1:
                raise ReadTimeoutError(endpoint_url="https://s3.example")
        raise ClientError(
            {
                "Error": {"Code": "NoSuchUpload"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            operation,
        )

    engine = _engine(attempts)
    # s3transfer runs real futures with its normal worker count. A lost response
    # means the object was published but retrying Complete returns NoSuchUpload.
    with patch("botocore.client.BaseClient._make_api_call", side_effect=request):
        if destination == "ours":
            assert engine.try_copy_object("source", "key", "key")
        else:
            with pytest.raises(S3MultipartCopyError, match="NoSuchUpload"):
                engine.try_copy_object("source", "key", "key")
    assert metadata.get("ch-backup-copy-id")
    assert calls == {
        "HeadObject": 2,
        "CreateMultipartUpload": 1,
        "UploadPartCopy": 2,
        "CompleteMultipartUpload": attempts,
        **({"AbortMultipartUpload": 1} if destination != "ours" else {}),
    }
