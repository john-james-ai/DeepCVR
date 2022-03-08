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
# Modified : Saturday, March 5th 2022, 9:59:35 am                                                  #
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

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #


class S3Downloader:
    """Download operator for Amazon S3 Resources.

    Args:
        bucket (str): S3 Bucket
        key: The S3 aws_access_key_id
        password: The S3 aws_secret_access_key
        folder: The folder within the bucket containing the resource
        destination (str): Director to which all resources are to be downloaded
        objects (list): list of objects to download. If None, all objects in bucket are downloaded.
            Default None.
        force (bool): Determines whether to force download if local version exists.
    """

    def __init__(
        self,
        bucket: str,
        key: str,
        password: str,
        folder: str,
        destination: str,
        objects: list = None,
        force: bool = False,
    ) -> None:
        self._bucket = bucket
        self._key = key
        self._password = password
        self._folder = folder
        self._destination = destination
        self._objects = objects
        self._force = force

        self._progressbar = None

    def execute(self) -> None:

        object_keys = self._objects if self._objects else self._list_bucket_contents()

        self._s3 = boto3.client(
            "s3", aws_access_key_id=self._key, aws_secret_access_key=self._password
        )

        os.makedirs(self._destination, exist_ok=True)

        for object_key in object_keys:
            destination = os.path.join(self._destination, os.path.basename(object_key))
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
        for object in bucket.objects.filter(Prefix=self._folder):
            logger.debug("Found {} in {}".format(object.key, self._folder))
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
