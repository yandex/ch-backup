"""
Steps related to s3.
"""

from behave import given

from tests.integration.helpers.s3 import (configure_s3_credentials,
                                          ensure_s3_bucket)


@given('a working s3')
def step_wait_for_s3_alive(context):
    """
    Ensure that s3 is ready to accept incoming requests, and
    the bucket specified in the config is created.
    """
    configure_s3_credentials(context)
    ensure_s3_bucket(context)
