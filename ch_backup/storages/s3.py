"""
Implementation of s3 storage
"""

import os
from tempfile import TemporaryFile

import boto3
import botocore.vendored.requests.packages.urllib3 as boto_urllib3
from botocore.errorfactory import ClientError

from .base import Loader


class S3Loader(Loader):
    """
    Loader for s3-like storage
    """

    def __init__(self, config, path_prefix=None):
        self._path_prefix = path_prefix if path_prefix else ''
        credentials_config = config['credentials']
        self._s3_session = boto3.session.Session(
            aws_access_key_id=credentials_config['access_key_id'],
            aws_secret_access_key=credentials_config['secret_access_key'],
        )

        self._s3_client = self._s3_session.client(
            service_name='s3',
            endpoint_url=credentials_config['endpoint_url'],
        )

        self._s3_bucket_name = credentials_config['bucket']

        if config.get('disable_ssl_warnings'):
            self.disable_boto_requests_warnings()

    @property
    def path_prefix(self):
        """
        path prefix property
        """
        return self._path_prefix

    @path_prefix.setter
    def path_prefix(self, value):
        self._path_prefix = value

    def upload_file(self, local_path, remote_path=None, path_prefix=None):
        if not remote_path:
            remote_path = local_path
        if path_prefix is None:
            path_prefix = self._path_prefix

        remote_path = os.path.join(path_prefix, remote_path).lstrip('/')
        with open(local_path, 'rb') as data:
            self._s3_client.upload_fileobj(data, self._s3_bucket_name,
                                           remote_path)
        return remote_path

    def upload_data(self, data, remote_path, path_prefix=None):
        if path_prefix is None:
            path_prefix = self._path_prefix

        remote_path = os.path.join(path_prefix, remote_path).lstrip('/')
        self._s3_client.put_object(
            Body=data, Bucket=self._s3_bucket_name, Key=remote_path)
        return remote_path

    def upload_dir(self, dir_path, path_prefix=None):
        if path_prefix is None:
            path_prefix = self._path_prefix

        uploaded_files = []
        for root, _, files in os.walk(dir_path):
            for file_name in files:

                dir_rel_path = os.path.relpath(root, dir_path)
                file_rel_path = os.path.join(
                    dir_rel_path,
                    file_name) if dir_rel_path != '.' else file_name
                file_abs_path = os.path.join(root, file_name)

                uploaded_files.append(
                    self.upload_file(
                        file_abs_path,
                        remote_path=file_rel_path,
                        path_prefix=path_prefix))

        return uploaded_files

    def download_file(self, remote_path, local_path, path_prefix=None):
        if path_prefix is None:
            path_prefix = self._path_prefix

        remote_path = os.path.join(path_prefix, remote_path).lstrip('/')
        return self._s3_client.download_file(self._s3_bucket_name, remote_path,
                                             local_path)

    def download_data(self, remote_path, path_prefix=None):
        if path_prefix is None:
            path_prefix = self._path_prefix

        remote_path = os.path.join(path_prefix, remote_path).lstrip('/')
        with TemporaryFile() as fileobj:
            self._s3_client.download_fileobj(self._s3_bucket_name, remote_path,
                                             fileobj)
            fileobj.seek(0)
            data = fileobj.read()
        return data

    def list_dir(self, remote_path, abs_path=True):
        contents = []
        paginator = self._s3_client.get_paginator('list_objects')
        for result in paginator.paginate(
                Bucket=self._s3_bucket_name,
                Prefix='{s3_path}/'.format(s3_path=remote_path),
                Delimiter='/'):
            if result.get('CommonPrefixes') is not None:
                for dir_prefix in result.get('CommonPrefixes'):
                    dir_path = dir_prefix.get('Prefix')
                    if not abs_path:
                        dir_path = os.path.relpath(dir_path, remote_path)
                    contents.append(dir_path)

            if result.get('Contents') is not None:
                for file_key in result.get('Contents'):
                    file_path = file_key.get('Key')
                    if not abs_path:
                        file_path = os.path.relpath(file_path, remote_path)
                    contents.append(file_path)

        return contents

    def path_exists(self, remote_path):
        try:
            self._s3_client.head_object(
                Bucket=self._s3_bucket_name, Key=remote_path)
            return True
        except ClientError:
            return False

    def download_dir(self, remote_path, local_path):
        paginator = self._s3_client.get_paginator('list_objects')

        for result in paginator.paginate(
                Bucket=self._s3_bucket_name,
                Prefix='{s3_path}/'.format(s3_path=remote_path)):
            if result.get('Contents') is not None:
                for file_key in result.get('Contents'):
                    key_rel_path = os.path.relpath(
                        file_key.get('Key'), remote_path)

                    local_file_path = os.path.join(local_path, key_rel_path)
                    local_file_dir = os.path.dirname(local_file_path)

                    if not os.path.exists(local_file_dir):
                        os.makedirs(local_file_dir, exist_ok=True)

                    self.download_file(file_key.get('Key'), local_file_path)

    def get_abs_path(self, rel_path):
        return os.path.join(self.path_prefix, rel_path)

    @staticmethod
    def disable_boto_requests_warnings():
        """
        Disable urllib warnings (annoys with self-signed ca)
        """
        boto_urllib3.disable_warnings(
            boto_urllib3.exceptions.InsecureRequestWarning)
