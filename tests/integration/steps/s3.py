"""
Steps related to s3.
"""

from behave import given

from tests.integration.helpers.s3 import ensure_s3_bucket, wait_for_s3_alive


@given('a working s3')
def step_wait_for_s3_alive(context):
    """
    Ensure that s3 is ready to accept incoming requests, and
    the bucket specified in the config is created.
    """
    wait_for_s3_alive(context)
    ensure_s3_bucket(context)
