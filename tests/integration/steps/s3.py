"""
Steps for interacting with S3.
"""

from behave import given

from tests.integration.modules.minio import (configure_s3_credentials, create_s3_bucket)


@given('a working s3')
def step_wait_for_s3_alive(context):
    """
    Ensure that s3 is ready to accept incoming requests, and
    the bucket specified in the config is created.
    """
    configure_s3_credentials(context)
    create_s3_bucket(context)
