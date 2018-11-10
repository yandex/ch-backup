"""
S3 client.
"""
import logging
from tempfile import TemporaryFile

import boto3
import botocore.vendored.requests.packages.urllib3 as boto_urllib3
from botocore.client import Config
from botocore.errorfactory import ClientError

from . import docker


class S3Client:
    """
    S3 client.
    """

    def __init__(self, context) -> None:
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
        self._disable_boto_requests_warnings()

    def upload_data(self, data: bytes, remote_path: str) -> None:
        """
        Upload given bytes or file-like object.
        """

        remote_path = remote_path.lstrip('/')
        self._s3_client.put_object(
            Body=data, Bucket=self._s3_bucket_name, Key=remote_path)

    def download_data(self, remote_path: str) -> str:
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

    def path_exists(self, remote_path: str) -> bool:
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
    def _disable_boto_requests_warnings() -> None:
        boto_urllib3.disable_warnings(
            boto_urllib3.exceptions.InsecureRequestWarning)

        for module_logger in ('boto3', 'botocore', 's3transfer', 'urllib3'):
            logging.getLogger(module_logger).setLevel(logging.CRITICAL)
