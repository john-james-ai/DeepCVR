#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /download.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, February 27th 2022, 8:31:48 am                                                #
# Modified : Saturday, March 5th 2022, 11:25:52 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Airflow operator responsible for downloading source data from an Amazon S3 resource. """
from dataclasses import dataclass
from airflow.models.baseoperator import BaseOperator
from deepcvr.data.download import S3Downloader


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DownloadConfig:
    task_id: str
    bucket: str
    key: str
    password: str
    folder: str
    destination: str
    objects: list = None
    force: bool = False


# ------------------------------------------------------------------------------------------------ #


class S3DownloadOperator(BaseOperator):
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
        **kwargs
    ) -> None:
        super(S3DownloadOperator, self).__init__(**kwargs)
        self._bucket = bucket
        self._key = key
        self._password = password
        self._folder = folder
        self._destination = destination
        self._objects = objects
        self._force = force

    def execute(self, context) -> None:
        s3_downloader = S3Downloader(
            bucket=self._bucket,
            key=self._key,
            password=self._password,
            folder=self._folder,
            destination=self._destination,
            objects=self._objects,
            force=self._force,
        )
        s3_downloader.execute()
