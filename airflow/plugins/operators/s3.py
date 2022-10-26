"""This module contains AWS S3 operators."""
from __future__ import annotations

import subprocess
import sys
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence
from datetime import datetime
from wsgiref.handlers import format_date_time

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


from hooks.s3 import S3Hook
from helpers.s3 import *

if TYPE_CHECKING:
    from airflow.utils.context import Context


BUCKET_DOES_NOT_EXIST_MSG = "Bucket with name: %s doesn't exist"


class S3DeleteObjectsOperator(BaseOperator):
    """
    To enable users to delete single object or multiple objects from
    a bucket using a single HTTP request.
   
    :param bucket: Name of the bucket in which you are going to delete object(s). (templated)
    :param keys: The key(s) to delete from S3 bucket. (templated)
        When ``keys`` is a string, it's supposed to be the key name of
        the single object to delete.
        When ``keys`` is a list, it's supposed to be the list of the
        keys to delete.
    :param prefix: Prefix of objects to delete. (templated)
        All objects matching this prefix in the bucket will be deleted.
    :param aws_conn_id: Connection id of the S3 connection to use
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:
        - ``False``: do not validate SSL certificates. SSL will still be used,
                 but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    """

    template_fields: Sequence[str] = ("keys", "bucket", "prefix")

    def __init__(
        self,
        *,
        bucket: str,
        keys: str | list | None = None,
        prefix: str | None = None,
        aws_conn_id: str = "aws_default",
        verify: str | bool | None = None,
        from_datetime: datetime | None = None,
        to_datetime: datetime | None = None,
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.bucket = bucket
        self.keys = keys
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime

        if not bool(keys is None) ^ bool(prefix is None):
            raise AirflowException("Either keys or prefix should be set.")
    


    def execute(self, context: Context):
        if not bool(self.keys is None) ^ bool(self.prefix is None):
            raise AirflowException("Either keys or prefix should be set.")

        if isinstance(self.keys, (list, str)) and not bool(self.keys):
            return
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        object_keys = self.keys or s3_hook.list_all_keys(bucket_name=self.bucket,
                                                            prefix=self.prefix,
                                                            from_datetime=self.from_datetime,
                                                            to_datetime=self.to_datetime)
        if object_keys:
            batches = chunks(object_keys, 1000)
            for batch in batches:
                s3_hook.delete_objects(bucket=self.bucket, keys=batch)