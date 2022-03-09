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
# Modified : Tuesday, March 8th 2022, 11:31:40 pm                                                  #
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
import tarfile
from botocore.exceptions import NoCredentialsError

from deepcvr.data.core import ETLDag, Task
from deepcvr.utils.io import load_csv, save_csv
from deepcvr.data import COLS_CORE_DATASET, COLS_COMMON_FEATURES_DATASET

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #


class S3Downloader(Task):
    """Download operator for Amazon S3 Resources.

    Args:
        mode (str): Either 'dev' or 'prod'.
        config (dict): Configuration dictionary that includes:
            bucket (str): S3 Bucket
            key: The S3 aws_access_key_id
            password: The S3 aws_secret_access_key
            folder: The folder within the bucket containing the resource
            destination (str): Director to which all resources are to be downloaded
            objects (list): list of objects to download. If None, all objects in bucket
            are downloaded. Default None.
            force (bool): Determines whether to force download if local version exists.
    """

    def __init__(self, config: dict, mode: str = "dev") -> None:
        super(S3Downloader, self).__init__(config=config, mode=mode)

        logger.debug("Mode: {}".format(self._mode))
        task_config = config[self._mode][self.__class__.__name__]

        logger.debug(task_config)

        self._bucket = task_config["bucket"]
        self._key = task_config["key"]
        self._password = task_config["password"]
        self._folder = task_config["folder"]
        self._destination = task_config["destination"]
        self._objects = task_config["objects"]
        self._force = task_config["force"]

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
        for object in bucket.objects.filter(Delimiter="/", Prefix=self._folder):
            logger.debug("Found {} in {}".format(object.key, self._folder))
            objects.append(object.key)
        return objects

    def _download(self, object_key: str, destination: str) -> None:
        """Downloads object designated by the object ke if not exists or force is True"""

        logger.debug("Downloading {} from bucket {}".format(object_key, self._bucket))

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


# ------------------------------------------------------------------------------------------------ #


class Decompress(Task):
    """Decompresses a gzip archive, stores the raw data

    Args:
        config (dict): Dictionary configuration including:
            source (str): The filepath to the source file to be decompressed
            destination (str): The destination directory into which data shall be stored.
            force (bool): Forces extraction even when files already exist.
        mode (str): Either 'dev' or 'prod'
    """

    def __init__(self, config: dict, mode: str = "dev") -> None:
        super(Decompress, self).__init__(config=config, mode=mode)

        self._source = config[self._mode][self.__class__.__name__]["source"]
        self._destination = config[self._mode][self.__class__.__name__]["destination"]
        self._force = config[self._mode][self.__class__.__name__]["force"]

    def execute(self) -> None:
        """Extracts and stores the data, then pushes filepaths to xCom."""
        logger.debug("\tSource: {}\tDestination: {}".format(self._source, self._destination))

        # Create destination if it doesn't exist
        os.makedirs(self._destination, exist_ok=True)

        # If all 4 raw files exist, it is assumed that the data have been downloaded
        n_files = len(os.listdir(self._destination))
        if n_files < 4:
            filenames = os.listdir(self._source)
            for filename in filenames:
                filepath = os.path.join(self._source, filename)
                tar = tarfile.open(filepath, "r:gz")
                logger.debug("Just opened {}".format(filepath))
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
        source (str): Input directory
        destination (str): Destination directory
        n_partitions (int): Number of partitions to designate
        force (bool): If True, overwrite existing data in the staged directory.

    """

    def __init__(self, config: dict, mode: str = "dev") -> None:
        super(Stage, self).__init__(config=config, mode=mode)

        self._source = config[self._mode][self.__class__.__name__]["source"]
        self._destination = config[self._mode][self.__class__.__name__]["destination"]
        self._n_partitions = config[self._mode][self.__class__.__name__]["n_partitions"]
        self._force = config[self._mode][self.__class__.__name__]["force"]

    def execute(self) -> None:

        filenames = os.listdir(self._source)

        for filename in filenames:
            source = os.path.join(self._source, filename)
            destination = os.path.join(self._destination, filename)

            if not os.path.exists(destination) or self._force:
                names = self._get_names(destination)

                data = load_csv(source, header=None, names=names)
                reindexed = data.reset_index()

                data["partition"] = reindexed.index % self._n_partitions

                logger.debug(
                    "Staging {} with {} observations".format(destination, str(data.shape[0]))
                )

                save_csv(data, filepath=destination, header=True, index=False)

    def _get_names(self, filename: str) -> list:
        if "common" in filename:
            return COLS_COMMON_FEATURES_DATASET
        else:
            return COLS_CORE_DATASET


# ================================================================================================ #


class ExtractDAG(ETLDag):
    """Directed acyclic graph for the extract phase of the ETL

    Args:
        mode (str): Mode is 'dev' for development or 'prod' for production data.
        config_filepath (str): The location of the

    """

    def __init__(self, config: dict, mode: str = "dev") -> None:
        super(ExtractDAG, self).__init__(config=config, mode=mode)
