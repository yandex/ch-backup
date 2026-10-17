"""Deletion pipelines must respect the S3 retry policy."""

import ssl
from unittest.mock import Mock

import pytest
import requests
from botocore.exceptions import ClientError

from ch_backup.config import DEFAULT_CONFIG
from ch_backup.storage.async_pipeline.stages.storage.delete_multiple_storage_stage import (
    DeleteMultipleStorageStage,
)
from ch_backup.storage.engine.s3 import S3RetryingError, S3StorageEngine
from ch_backup.storage.engine.s3.s3_client_factory import (
    S3ClientCachedFactory,
)


def _engine(bulk: bool = True) -> S3StorageEngine:
    config = {
        **DEFAULT_CONFIG["storage"],
        "bulk_delete_enabled": bulk,
        "retry_max_attempts": 2,
    }
    engine = S3StorageEngine(config)
    getattr(engine, "_retry").sleep_function = Mock()
    return engine


def _stage(engine: S3StorageEngine) -> DeleteMultipleStorageStage:
    return DeleteMultipleStorageStage(
        {"bulk_delete_chunk_size": 1000}, ["a", "b"], engine
    )


def _s3_error(status: int, code: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "DeleteObjects",
    )


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("status", [400, 403, 507])
def test_deletion_pipeline_does_not_retry_permanent_resolver_error(
    bulk: bool, status: int
) -> None:
    engine = _engine(bulk)
    response = requests.Response()
    response.status_code = status
    error = requests.HTTPError(response=response)
    source = Mock(create_s3_client=Mock(side_effect=error))
    setattr(engine, "_s3_client_factory", S3ClientCachedFactory(source))

    with pytest.raises(requests.HTTPError) as exc:
        _stage(engine).on_done()

    assert exc.value is error
    source.create_s3_client.assert_called_once()
    getattr(engine, "_retry").sleep_function.assert_not_called()


def test_deletion_pipeline_does_not_retry_bad_certificate() -> None:
    engine = _engine()
    error = requests.exceptions.SSLError(
        ssl.SSLCertVerificationError(1, "bad certificate")
    )
    source = Mock(create_s3_client=Mock(side_effect=error))
    setattr(engine, "_s3_client_factory", S3ClientCachedFactory(source))

    with pytest.raises(requests.exceptions.SSLError) as exc:
        _stage(engine).on_done()

    assert exc.value is error
    source.create_s3_client.assert_called_once()
    getattr(engine, "_retry").sleep_function.assert_not_called()


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("status,code", [(403, "AccessDenied"), (507, "InternalError")])
def test_deletion_pipeline_does_not_retry_permanent_s3_error(
    bulk: bool, status: int, code: str
) -> None:
    engine = _engine(bulk)
    client = Mock()
    request = client.delete_objects if bulk else client.delete_object
    error = _s3_error(status, code)
    request.side_effect = error
    source = Mock(create_s3_client=Mock(return_value=client))
    setattr(engine, "_s3_client_factory", S3ClientCachedFactory(source))

    with pytest.raises(ClientError) as exc:
        _stage(engine).on_done()

    assert exc.value is error
    request.assert_called_once()
    getattr(engine, "_retry").sleep_function.assert_not_called()


@pytest.mark.parametrize("recover", [False, True])
def test_bulk_delete_uses_one_retry_budget(recover: bool) -> None:
    engine = _engine()
    client = Mock()
    error = _s3_error(503, "ServiceUnavailable")
    client.delete_objects.side_effect = [error, {} if recover else error]
    source = Mock(create_s3_client=Mock(return_value=client))
    setattr(engine, "_s3_client_factory", S3ClientCachedFactory(source))

    if recover:
        _stage(engine).on_done()
    else:
        with pytest.raises(S3RetryingError):
            _stage(engine).on_done()

    assert client.delete_objects.call_count == 2
    getattr(engine, "_retry").sleep_function.assert_called_once()
