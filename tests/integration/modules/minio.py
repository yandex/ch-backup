"""
Interface to Minio S3 server.
"""
import json
import os

from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_fixed)

from .docker import copy_container_dir, get_container


class MinioException(Exception):
    """
    Minion exception.
    """

    def __init__(self, response):
        super().__init__(self._fmt_message(response))
        self.response = response

    @staticmethod
    def _fmt_message(response):
        try:
            error = response['error']
            message = '{0} Cause: {1}'.format(error['message'],
                                              error['cause']['message'])

            code = error['cause']['error'].get('Code')
            if code:
                message = '{0} [{1}]'.format(message, code)

            return message

        except Exception:
            return 'Failed with response: {0}'.format(response)


class BucketAlreadyOwnedByYou(MinioException):
    """
    BucketAlreadyOwnedByYou Minion exception.
    """


@retry(
    retry=retry_if_exception_type(MinioException),
    wait=wait_fixed(0.5),
    stop=stop_after_attempt(360))
def configure_s3_credentials(context):
    """
    Configure S3 credentials in mc (Minio client).
    """
    access_key = context.conf['s3']['access_key_id']
    secret_key = context.conf['s3']['access_secret_key']
    _mc_execute(
        context, 'config host add local http://localhost:9000 {0} {1}'.format(
            access_key, secret_key))


def ensure_s3_bucket(context):
    """
    Ensure S3 has the bucket specified in the config.
    """
    bucket = context.conf['s3']['bucket']
    try:
        _mc_execute(context, 'mb local/{0}'.format(bucket))
    except BucketAlreadyOwnedByYou:
        pass


def export_s3_data(context, path):
    """
    Export S3 data to the specified directory.
    """
    local_dir = os.path.join(path, 'minio')
    copy_container_dir(_container(context), '/export', local_dir)


def _container(context):
    return get_container(context, context.conf['s3']['container'])


def _mc_execute(context, command):
    """
    Execute mc (Minio client) command.
    """
    output = _container(context).exec_run(
        'mc --json {0}'.format(command)).decode()

    response = json.loads(output)
    if response['status'] == 'success':
        return response

    error_code = response['error']['cause']['error'].get('Code')
    exception_types = {
        'BucketAlreadyOwnedByYou': BucketAlreadyOwnedByYou,
    }
    raise exception_types.get(error_code, MinioException)(response)
