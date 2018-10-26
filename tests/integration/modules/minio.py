"""
Interface to Minio S3 server.
"""
import json

from tenacity import retry, stop_after_attempt, wait_fixed

from . import docker


@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(360))
def configure_s3_credentials(context):
    """
    Configure S3 credentials in mc (Minio client).
    """
    access_key = context.conf['s3']['access_key_id']
    secret_key = bucket = context.conf['s3']['access_secret_key']

    response = _mc_execute(
        context, 'config host add local http://localhost:9000 {0} {1}'.format(
            access_key, secret_key))
    if response['status'] != 'success':
        raise RuntimeError('Cannot configure s3 credentials {0}: {1}'.format(
            bucket, response))


def ensure_s3_bucket(context):
    """
    Ensure S3 has the bucket specified in the config.
    """
    bucket = context.conf['s3']['bucket']

    response = _mc_execute(context, 'mb local/{0}'.format(bucket))
    if response['status'] != 'success':
        error_code = response['error']['cause']['error']['Code']
        if error_code != 'BucketAlreadyOwnedByYou':
            raise RuntimeError('Cannot create bucket {0}: {1}'.format(
                bucket, response))


def _mc_execute(context, command):
    """
    Execute mc (Minio client) command.
    """
    container = docker.get_container(context, context.conf['s3']['container'])
    output = container.exec_run('mc --json {0}'.format(command)).decode()
    return json.loads(output)
