"""
The module provides type-hints.
"""
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import (DeleteObjectsOutputTypeDef, ObjectIdentifierTypeDef)
else:
    # TODO: Use module level __getattr_() as fallback (PEP 562) in Python 3.7+
    S3Client = Any
    DeleteObjectsOutputTypeDef = Any
