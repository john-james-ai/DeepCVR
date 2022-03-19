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
# Modified : Saturday, March 19th 2022, 5:11:01 am                                                 #
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

from deepcvr.data.base import Task
from deepcvr.utils.decorators import task_event
from deepcvr.utils.io import ParquetIO, CsvIO

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

    @task_event
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

    @task_event
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
class Preprocess(Task):
    """Adds column names and casts the data types

    Args:
        task_id (int): The task sequence number
        task_name (str): Brief title for the task
        params (dict): The dictionary of parameters for the task, including:
            - columns (list): List of column names for the dataset
            - dtypes (dict): Dictionary mapping column names to data types
            - source (str): The filepath for the file to be preprocessed
            - destination (str): The filepath where the preprocessed file is to be stored
            - force (bool): If False, don't execute if data already exists at destination.
    """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(Preprocess, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @task_event
    def execute(self, context: Any = None) -> Any:
        """Executes the preprocessing task

        Adds column names and casts the data types

        Args:
            context (dict): Parameters passed from the pipeline
        """
        source = self._params["source"]
        destination = self._params["destination"]
        force = self._params["force"]
        columns = self._params["columns"]
        dtypes = self._params["dtypes"]

        # Add partitioning variables to the file
        partitioning_cols = []
        if "sample_id" in columns:
            partitioning_cols.append("sample_id")
        if "common_features_index" in columns:
            partitioning_cols.append("common_features_index")

        os.makedirs(os.path.dirname(destination), exist_ok=True)

        if not os.path.exists(destination) or force:

            io = CsvIO()
            df = io.load(source, header=None, names=columns, index_col=False, dtype=dtypes)
            io = ParquetIO()
            io.save(
                df,
                filepath=destination,
                engine="auto",
                index=False,
                partition_cols=partitioning_cols,
            )


# ------------------------------------------------------------------------------------------------ #
class FeatureExtraction(Task):
    """Extracts core features from dataset in preparation for feature transformation"""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(FeatureExtraction, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @task_event
    def execute(self, context: Any = None) -> Any:
        """Creates the core feature dataset"""

        io = ParquetIO()
        df = io.load(filepath=self._params["source"], engine="auto", partition_cols=["sample_id"],)

        features = df[["sample_id", "num_features", "features_list"]]

        io.save(
            features,
            filepath=self._params["destination"],
            engine="auto",
            partition_cols=["sample_id"],
        )
