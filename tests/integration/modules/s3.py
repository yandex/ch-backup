"""
S3 client.
"""
import logging
from typing import List

import boto3
from botocore.client import Config
from botocore.errorfactory import ClientError

from . import docker
from .typing import ContextT


class S3Client:
    """
    S3 client.
    """
    def __init__(self, context: ContextT) -> None:
        config = context.conf['s3']
        boto_config = config['boto_config']
        self._s3_session = boto3.session.Session(aws_access_key_id=config['access_key_id'],
                                                 aws_secret_access_key=config['access_secret_key'])

        host, port = docker.get_exposed_port(docker.get_container(context, context.conf['s3']['container']),
                                             context.conf['s3']['port'])
        endpoint_url = f'http://{host}:{port}'
        self._s3_client = self._s3_session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            config=Config(s3={
                'addressing_style': boto_config['addressing_style'],
                'region_name': boto_config['region_name'],
            }))
        self._s3_bucket_name = config['bucket']

        for module_logger in ('boto3', 'botocore', 's3transfer', 'urllib3'):
            logging.getLogger(module_logger).setLevel(logging.CRITICAL)

    def upload_data(self, data: bytes, remote_path: str) -> None:
        """
        Upload given bytes or file-like object.
        """
        remote_path = remote_path.lstrip('/')
        self._s3_client.put_object(Body=data, Bucket=self._s3_bucket_name, Key=remote_path)

    def delete_data(self, remote_path: str) -> None:
        """
        Delete file from storage.
        """
        remote_path = remote_path.lstrip('/')
        self._s3_client.delete_object(Bucket=self._s3_bucket_name, Key=remote_path)

    def path_exists(self, remote_path: str) -> bool:
        """
        Check if remote path exists.
        """
        try:
            self._s3_client.head_object(Bucket=self._s3_bucket_name, Key=remote_path)
            return True
        except ClientError:
            return False

    def list_objects(self, prefix: str) -> List[str]:
        """
        List all objects with given prefix.
        """
        contents = []
        paginator = self._s3_client.get_paginator('list_objects')
        list_object_kwargs = dict(Bucket=self._s3_bucket_name, Prefix=prefix)

        for result in paginator.paginate(**list_object_kwargs):
            if result.get('CommonPrefixes') is not None:
                for dir_prefix in result.get('CommonPrefixes'):
                    contents.append(dir_prefix.get('Prefix'))

            if result.get('Contents') is not None:
                for file_key in result.get('Contents'):
                    contents.append(file_key.get('Key'))

        return contents
