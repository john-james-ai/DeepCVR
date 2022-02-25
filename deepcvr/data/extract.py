#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /extractor.py                                                                         #
# Language : Python 3.10.2                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, February 14th 2022, 12:32:13 pm                                               #
# Modified : Friday, February 25th 2022, 4:01:42 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import os
import boto3
import logging
import progressbar
from airflow.models.baseoperator import BaseOperator
from botocore.exceptions import NoCredentialsError

from deepcvr.utils.config import S3Config

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #


class S3Downloader(BaseOperator):
    """Download operator for Amazon S3 Resources

    Args:
        bucket (str): The name of the S3 bucket
        destination (str): Director to which all resources are to be downloaded
    """

    def __init__(self, bucket: str, destination: str, **kwargs) -> None:
        self._bucket = bucket
        self._destination = destination
        config = S3Config()
        self._s3 = boto3.client(
            "s3", aws_access_key_id=config.key, aws_secret_access_key=config.secret
        )
        self._progressbar = None

    def execute(self, context=None) -> None:

        object_keys = self._list_bucket_contents()

        for object_key in object_keys:
            self._download(object_key)

    def _list_bucket_contents(self) -> list:
        """Returns a list of objects in the designated bucket"""
        objects = []
        bucket = self._s3.Bucket(self._bucket)
        for object in bucket.objects.all():
            objects.append(object.key)
        return objects

    def _download(self, object_key: str) -> None:
        """Downloads object designated by the object key"""

        response = self._s3.head_object(Bucket=self._bucket, Key=object_key)
        size = response["ContentLength"]

        self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
        self._progressbar.start()

        filepath = os.path.join(self._destination, object_key)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        try:
            self._s3.download_file(
                self._bucket, object_key, filepath, Callback=self._download_callback
            )
            logger.info("Download of {} Complete!".format(object_key))
        except FileNotFoundError:
            msg = "Filepath: {} not found".format(filepath)
            raise FileNotFoundError(msg)
        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(self._bucket)
            raise NoCredentialsError(msg)

    def _download_callback(self, size):
        self._progressbar.update(self._progressbar.currval + size)


# ------------------------------------------------------------------------------------------------ #


class ExtractOperator(BaseOperator):
    """Decompresses a gzip archive, stores the raw data and pushes the filepaths to xCom

    Args:
        source (str): The source filepath
        destination (str): The destination directory
        kwargs (dict): Default parameters
    """

    def __init__(self, source: str, destination: str, **kwargs) -> None:
        super(ExtractOperator, self).__init__(**kwargs)
        self._source = source
        self._destination = destination

    def execute(self, context=None) -> None:
        """Extracts and stores the data, then pushes filepaths to xCom. """
        from deepcvr.data.extract import Extractor

        extractor = Extractor(source=self._source, destination=self._destination)
        filepaths = extractor.execute()

        # Push the extracted filepaths to xCom
        ti = context["ti"]
        ti.xcom_push("filepaths", filepaths)
