"""
Steps related to s3.
"""

from behave import given
from retrying import retry

from tests.integration.helpers import docker


@given('a working s3 on {node_name}')
@retry(wait_fixed=200, stop_max_attempt_number=25)
def step_wait_for_s3_alive(context, node_name):
    """
    Wait until s3 is ready to accept incoming requests.
    """
    context.s3_container = docker.get_container(context, node_name)
    output = context.s3_container.exec_run('mc admin info fake-s3').decode()
    if 'online' not in output:
        raise RuntimeError('s3 is not available: ' + output)


@given('s3 bucket {bucket_name}')
def step_ensure_s3_bucket(context, bucket_name):
    """
    Create s3 bucket
    """
    output = context.s3_container.exec_run(
        'mc mb fake-s3/{bucket_name}'.format(
            bucket_name=bucket_name)).decode()

    if all(
            log not in output
            for log in ('created successfully', 'already own it')):
        raise RuntimeError(
            'Can not create bucket %s: %s' % (bucket_name, output))
