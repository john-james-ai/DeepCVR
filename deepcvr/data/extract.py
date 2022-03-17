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
# Modified : Wednesday, March 16th 2022, 7:10:39 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import os
import boto3
import progressbar
import tarfile
from botocore.exceptions import NoCredentialsError
from typing import Any
from dotenv import load_dotenv

from deepcvr.data.core import Task
from deepcvr.utils.io import CsvIO
from deepcvr.data import COLS_CORE_DATASET, COLS_COMMON_FEATURES_DATASET
from deepcvr.utils.decorators import event

# ------------------------------------------------------------------------------------------------ #
# Uncomment for debugging
# import logging
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class S3Downloader(Task):
    """Download operator for Amazon S3 Resources.

    Args:
        task_id (int): Task sequence in dag.
        task_name (str): name of task
        params (dict): Parameters required by the task, including:
          bucket (str): The Amazon S3 bucket name
          key (str): The access key to the S3 bucket
          password (str): The secret access key to the S3 bucket
          folder (str): The folder within the bucket for the data
          destination (str): The folder to which the data is downloaded
          force (bool): If True, will execute and overwrite existing data.
    """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(S3Downloader, self).__init__(task_id=task_id, task_name=task_name, params=params)

        self._bucket = params["bucket"]
        self._folder = params["folder"]
        self._destination = params["destination"]
        self._force = params["force"]

        self._progressbar = None

    @event
    def execute(self, context: Any = None) -> Any:

        load_dotenv()

        object_keys = self._list_bucket_contents()

        s3access = os.getenv("S3ACCESS")
        s3password = os.getenv("S3PASSWORD")

        self._s3 = boto3.client("s3", aws_access_key_id=s3access, aws_secret_access_key=s3password)

        os.makedirs(self._destination, exist_ok=True)

        for object_key in object_keys:
            destination = os.path.join(self._destination, os.path.basename(object_key))

            if not os.path.exists(destination) or self._force:
                self._download(object_key, destination)

    def _list_bucket_contents(self) -> list:
        """Returns a list of objects in the designated bucket"""

        objects = []
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self._bucket)
        for object in bucket.objects.filter(Delimiter="/t", Prefix=self._folder):
            if not object.key.endswith("/"):  # Skip objects that are just the folder name
                objects.append(object.key)

        return objects

    def _download(self, object_key: str, destination: str) -> None:
        """Downloads object designated by the object ke if not exists or force is True"""
        response = self._s3.head_object(Bucket=self._bucket, Key=object_key)
        size = response["ContentLength"]

        self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
        self._progressbar.start()

        try:
            self._s3.download_file(
                self._bucket, object_key, destination, Callback=self._download_callback
            )

        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(self._bucket)
            raise NoCredentialsError(msg)

    def _download_callback(self, size):
        self._progressbar.update(self._progressbar.currval + size)


# ------------------------------------------------------------------------------------------------ #


class Decompress(Task):
    """Decompresses a gzip archive, stores the raw data

    Args:
        task_id (int): Task sequence in dag.
        task_name (str): name of task
        params (dict): Parameters required by the task, including:
          source (str): The source directory containing the gzip files
          destination (str): The directory into which the decompressed data is to be stored
          force (bool): If True, will execute and overwrite existing data.
    """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(Decompress, self).__init__(task_id=task_id, task_name=task_name, params=params)

        self._source = params["source"]
        self._destination = params["destination"]
        self._force = params["force"]

    @event
    def execute(self, context: Any = None) -> Any:
        """Extracts and stores the data, then pushes filepaths to xCom."""

        # Create destination if it doesn't exist
        os.makedirs(self._destination, exist_ok=True)

        # If all 4 raw files exist, it is assumed that the data have been downloaded
        n_files = len(os.listdir(self._destination))
        if n_files < 4:
            filenames = os.listdir(self._source)
            for filename in filenames:
                filepath = os.path.join(self._source, filename)
                tar = tarfile.open(filepath, "r:gz")
                tar.extractall(self._destination)

    def _not_exists_or_force(self, member_name: str) -> bool:
        """Returns true if the file doesn't exist or force is True."""
        filepath = os.path.join(self._destination, member_name)
        return not os.path.exists(filepath) or self._force

    def _is_csvfile(self, filename: str) -> bool:
        """Returns True if filename is a csv file, returns False otherwise."""
        return ".csv" in filename


# ------------------------------------------------------------------------------------------------ #


class Stage(Task):
    """Add columns and partition id to the data and stores it in the staged directory

    Args:
        task_id (int): Task sequence in dag.
        task_name (str): name of task
        params (dict): Parameters required by the task, including:
          source (str): The directory containing the input data
          destination (str): The directory into which the staged data is stored
          n_partitions (int): The number of partitions to assign
          force (bool): If True, will execute and overwrite existing data.
    """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(Stage, self).__init__(task_id=task_id, task_name=task_name, params=params)

        self._source = params["source"]
        self._destination = params["destination"]
        self._n_partitions = params["n_partitions"]
        self._force = params["force"]

    @event
    def execute(self, context: Any = None) -> Any:

        io = CsvIO()

        filenames = os.listdir(self._source)

        for filename in filenames:
            source = os.path.join(self._source, filename)
            destination = os.path.join(self._destination, filename)

            if not os.path.exists(destination) or self._force:
                names = self._get_names(destination)

                data = io.load(source, header=None, names=names)
                reindexed = data.reset_index()

                data["partition"] = reindexed.index % self._n_partitions

                io.save(data, filepath=destination, header=True, index=False)

    def _get_names(self, filename: str) -> list:
        if "common" in filename:
            return COLS_COMMON_FEATURES_DATASET
        else:
            return COLS_CORE_DATASET


# %%
