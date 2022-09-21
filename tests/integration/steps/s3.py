"""
Steps for interacting with S3.
"""

from behave import given, then
from hamcrest import assert_that, equal_to

from tests.integration.modules import s3
from tests.integration.modules.minio import (configure_s3_credentials, create_s3_buckets)


@given('a working s3')
def step_wait_for_s3_alive(context):
    """
    Ensure that s3 is ready to accept incoming requests, and
    the bucket specified in the config is created.
    """
    configure_s3_credentials(context)
    create_s3_buckets(context)


@then('s3 contains {count:d} objects')
def step_s3_contains_files(context, count):
    s3_client = s3.S3Client(context)
    objects = s3_client.list_objects('/')
    assert_that(len(objects), equal_to(count), f'Objects count = {len(objects)}, expected {count}, objects {objects}')


@then('s3 bucket {bucket} contains {count:d} objects')
def step_cloud_storage_bucket_contains_files(context, bucket, count):
    s3_client = s3.S3Client(context, bucket)
    objects = s3_client.list_objects('/')
    assert_that(len(objects), equal_to(count), f'Objects count = {len(objects)}, expected {count}, objects {objects}')
