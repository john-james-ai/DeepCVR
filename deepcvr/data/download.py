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
# Modified : Sunday, February 27th 2022, 8:40:14 am                                                #
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
from botocore.exceptions import NoCredentialsError

from deepcvr.utils.config import S3Config

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #


class S3Downloader:
    """Download operator for Amazon S3 Resources

    Args:
        credentials (S3Config): An S3 configuration object containing access credentials.
        bucket (str): The name of the S3 bucket
        destination (str): Director to which all resources are to be downloaded
        force (bool): Determines whether to force download if local version exists.
    """

    def __init__(
        self, credentials: S3Config, bucket: str, destination: str, force: bool = False
    ) -> None:
        self._bucket = bucket
        self._destination = destination
        self._force = force
        self._s3 = boto3.client(
            "s3", aws_access_key_id=credentials.key, aws_secret_access_key=credentials.secret
        )
        self._progressbar = None

    def execute(self) -> None:

        object_keys = self._list_bucket_contents()

        for object_key in object_keys:
            destination = os.path.join(self._destination, object_key)
            if not os.path.exists(destination) or self._force:
                self._download(object_key, destination)
            else:
                logger.info(
                    "Bucket resource {} already exists and was not downloaded.".format(destination)
                )

    def _list_bucket_contents(self) -> list:
        """Returns a list of objects in the designated bucket"""
        objects = []
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self._bucket)
        for object in bucket.objects.all():
            objects.append(object.key)
        return objects

    def _download(self, object_key: str, destination: str) -> None:
        """Downloads object designated by the object ke if not exists or force is True"""

        response = self._s3.head_object(Bucket=self._bucket, Key=object_key)
        size = response["ContentLength"]

        self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
        self._progressbar.start()

        os.makedirs(os.path.dirname(destination), exist_ok=True)
        try:
            self._s3.download_file(
                self._bucket, object_key, destination, Callback=self._download_callback
            )
            logger.info("Download of {} Complete!".format(object_key))
        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(self._bucket)
            raise NoCredentialsError(msg)

    def _download_callback(self, size):
        self._progressbar.update(self._progressbar.currval + size)
