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
# Modified : Sunday, February 27th 2022, 8:57:24 am                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Airflow operator responsible for downloading source data from an Amazon S3 resource. """
from airflow.models.baseoperator import BaseOperator
from deepcvr.data.download import S3Downloader
from deepcvr.utils.config import S3Config

# ------------------------------------------------------------------------------------------------ #


class S3DownloadOperator(BaseOperator):
    """Download operator for Amazon S3 Resources. Wraps an S3Downloader object.

    Args:
        credentials (S3Config): An S3 configuration object containing access credentials.
        bucket (str): The name of the S3 bucket
        destination (str): Director to which all resources are to be downloaded
        force (bool): Determines whether to force download if local version exists.
    """

    def __init__(
        self, credentials: S3Config, bucket: str, destination: str, force: bool = False, **kwargs
    ) -> None:
        super(S3DownloadOperator, self).__init__(**kwargs)
        self._s3_downloader = S3Downloader(
            credentials=credentials, bucket=bucket, destination=destination, force=force
        )

    def execute(self, context) -> None:
        self._s3_downloader.execute()
