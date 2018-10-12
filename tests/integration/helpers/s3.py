"""
Simple s3-client
"""

import logging
from tempfile import TemporaryFile

import boto3
import botocore.vendored.requests.packages.urllib3 as boto_urllib3
from botocore.client import Config
from botocore.errorfactory import ClientError
from retrying import retry

from . import docker


class S3Client:
    """
    S3-compatible simple client
    """

    def __init__(self, context):
        config = context.conf['s3']
        boto_config = config['boto_config']
        self._s3_session = boto3.session.Session(
            aws_access_key_id=config['access_key_id'],
            aws_secret_access_key=config['access_secret_key'])

        host, port = docker.get_exposed_port(
            docker.get_container(context, context.conf['s3']['container']),
            context.conf['s3']['port'])

        endpoint_url = 'http://{0}:{1}'.format(host, port)
        self._s3_client = self._s3_session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            config=Config(
                s3={
                    'addressing_style': boto_config['addressing_style'],
                    'region_name': boto_config['region_name'],
                }))

        self._s3_bucket_name = config['bucket']
        self.disable_boto_requests_warnings()

    def upload_data(self, data, remote_path):
        """
        Upload given bytes or file-like object.
        """

        remote_path = remote_path.lstrip('/')
        self._s3_client.put_object(
            Body=data, Bucket=self._s3_bucket_name, Key=remote_path)
        return remote_path

    def download_data(self, remote_path):
        """
        Download file from storage and return its content as a string.
        """

        remote_path = remote_path.lstrip('/')
        with TemporaryFile() as fileobj:
            self._s3_client.download_fileobj(self._s3_bucket_name, remote_path,
                                             fileobj)
            fileobj.seek(0)
            data = fileobj.read()
        return data

    def path_exists(self, remote_path):
        """
        Check if remote path exists.
        """
        try:
            self._s3_client.head_object(
                Bucket=self._s3_bucket_name, Key=remote_path)
            return True
        except ClientError:
            return False

    @staticmethod
    def disable_boto_requests_warnings():
        """
        Disable urllib warnings (annoys with self-signed ca)
        """
        boto_urllib3.disable_warnings(
            boto_urllib3.exceptions.InsecureRequestWarning)

        for module_logger in ('boto3', 'botocore', 's3transfer', 'urllib3'):
            logging.getLogger(module_logger).setLevel(logging.CRITICAL)


@retry(wait_fixed=200, stop_max_attempt_number=25)
def wait_for_s3_alive(context):
    """
    Ensure that s3 is ready to accept incoming requests.
    """
    output = get_s3_container(context).exec_run(
        'mc admin info fake-s3').decode()
    if 'online' not in output:
        raise RuntimeError('s3 is not available: ' + output)


def ensure_s3_bucket(context):
    """
    Ensure s3 has the bucket specified in the config.
    """
    bucket = (context.conf['s3']['bucket'])

    output = get_s3_container(context).exec_run(
        'mc mb fake-s3/{0}'.format(bucket)).decode()

    if all(
            log not in output
            for log in ('created successfully', 'already own it')):
        raise RuntimeError('Can not create bucket {0}: {1}'.format(
            bucket, output))


def get_s3_container(context):
    """
    Get S3 Docker container.
    """
    return docker.get_container(context, context.conf['s3']['container'])
