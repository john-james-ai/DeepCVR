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
# Modified : Sunday, March 13th 2022, 5:22:47 pm                                                   #
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
import inspect
import tarfile
from botocore.exceptions import NoCredentialsError
from typing import Any

from deepcvr.data.core import Task
from deepcvr.utils.io import CsvIO
from deepcvr.data import COLS_CORE_DATASET, COLS_COMMON_FEATURES_DATASET
from deepcvr.data.metabase import Asset, Event
from deepcvr.data.metabase import EventParams

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d-%Y %H:%M",
    filename="logs/metevent.log",
    filemode="w",
)
logger = logging.getLogger(__name__)
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
        self._key = params["key"]
        self._password = params["password"]
        self._folder = params["folder"]
        self._destination = params["destination"]
        self._force = params["force"]

        self._progressbar = None

    def execute(self, context: Any = None) -> Any:

        logger.info(
            "Started {} {} Source: {} Destination: {}".format(
                self.__class__.__name__, inspect.stack()[0][3], self._bucket, self._destination,
            ),
        )

        object_keys = self._list_bucket_contents()

        self._s3 = boto3.client(
            "s3", aws_access_key_id=self._key, aws_secret_access_key=self._password
        )
        logger.debug("Created boto3 client.")

        os.makedirs(self._destination, exist_ok=True)
        logger.debug("Created {} directory".format(self._destination))

        asset = Asset()

        for object_key in object_keys:
            destination = os.path.join(self._destination, os.path.basename(object_key))

            logger.debug("Pending download of {}".format(destination))

            if not os.path.exists(destination) or self._force:
                self._download(object_key, destination)
            else:
                logger.debug(
                    "Bucket resource {} already exists and was not downloaded.".format(destination)
                )

            desc = object_key + " from ALI-CCP Dataset"
            asset.add(name=object_key, desc=desc, uri=destination)

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def _list_bucket_contents(self) -> list:
        """Returns a list of objects in the designated bucket"""
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        objects = []
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self._bucket)
        for object in bucket.objects.filter(Delimiter="/t", Prefix=self._folder):
            if not object.key.endswith("/"):  # Skip objects that are just the folder name
                logger.debug("Found {} in {}".format(object.key, self._folder))
                objects.append(object.key)

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        return objects

    def _download(self, object_key: str, destination: str) -> None:
        """Downloads object designated by the object ke if not exists or force is True"""
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        logger.debug("Downloading {} from bucket {}".format(object_key, self._bucket))

        response = self._s3.head_object(Bucket=self._bucket, Key=object_key)
        size = response["ContentLength"]

        self._progressbar = progressbar.progressbar.ProgressBar(maxval=size)
        self._progressbar.start()

        try:
            self._s3.download_file(
                self._bucket, object_key, destination, Callback=self._download_callback
            )
            logger.debug("Download of {} Complete!".format(object_key))
        except NoCredentialsError:
            msg = "Credentials not available for {} bucket".format(self._bucket)
            raise NoCredentialsError(msg)

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def _download_callback(self, size):
        self._progressbar.update(self._progressbar.currval + size)

    def _register_events(self) -> None:
        """Registers the event with metadata"""
        # Load asset and event tables
        asset = Asset()
        event = Event()

        files = os.listdir(self._destination)
        for file in files:
            name = os.path.basename(file)
            desc = name + " external source data"
            uri = os.path.join(self._destination, file)
            asset.add(name=name, desc=desc, uri=uri)
            params = EventParams(
                module=__name__,
                method="execute",
                classname=__class__.__name__,
                action="create_data_source",
                param1=name,
                param2=self._destination,
            )
            event.add(event=params)


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

    def execute(self, context: Any = None) -> Any:
        """Extracts and stores the data, then pushes filepaths to xCom."""
        logger.info(
            "Started {} {} Source: {} Destination: {}".format(
                self.__class__.__name__, inspect.stack()[0][3], self._source, self._destination,
            ),
        )
        logger.debug("\tSource: {}\tDestination: {}".format(self._source, self._destination))

        # Create destination if it doesn't exist
        os.makedirs(self._destination, exist_ok=True)

        # Obtain the event

        # If all 4 raw files exist, it is assumed that the data have been downloaded
        n_files = len(os.listdir(self._destination))
        if n_files < 4:
            filenames = os.listdir(self._source)
            for filename in filenames:
                filepath = os.path.join(self._source, filename)
                tar = tarfile.open(filepath, "r:gz")
                logger.debug("Just opened {}".format(filepath))
                tar.extractall(self._destination)

        self._register_events()
        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def _not_exists_or_force(self, member_name: str) -> bool:
        """Returns true if the file doesn't exist or force is True."""
        filepath = os.path.join(self._destination, member_name)
        return not os.path.exists(filepath) or self._force

    def _is_csvfile(self, filename: str) -> bool:
        """Returns True if filename is a csv file, returns False otherwise."""
        return ".csv" in filename

    def _register_events(self) -> None:
        """Registers the event with metadata"""
        # Load asset and event tables
        asset = Asset()
        event = Event()

        files = os.listdir(self._destination)
        for file in files:
            name = os.path.basename(file)
            desc = name + " raw data"
            uri = os.path.join(self._destination, file)
            asset.add(name=name, desc=desc, uri=uri)
            params = EventParams(
                module=__name__,
                method="execute",
                classname=__class__.__name__,
                action="create_raw_data",
                param1=name,
                param2=self._destination,
            )
            event.add(event=params)


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

    def execute(self, context: Any = None) -> Any:

        logger.info(
            "Started {} {} Source: {} Destination: {} Partitions: {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                self._source,
                self._destination,
                str(self._n_partitions),
            )
        )

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

                logger.debug(
                    "Staging {} with {} observations".format(destination, str(data.shape[0]))
                )

                io.save(data, filepath=destination, header=True, index=False)

            logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def _get_names(self, filename: str) -> list:
        if "common" in filename:
            return COLS_COMMON_FEATURES_DATASET
        else:
            return COLS_CORE_DATASET

    def _register_events(self) -> None:
        """Registers the event with metadata"""
        # Load asset and event tables
        asset = Asset()
        event = Event()

        files = os.listdir(self._destination)
        for file in files:
            name = os.path.basename(file)
            desc = name + " staged data"
            uri = os.path.join(self._destination, file)
            asset.add(name=name, desc=desc, uri=uri)
            params = EventParams(
                module=__name__,
                method="execute",
                classname=__class__.__name__,
                action="create_staged_data",
                param1=name,
                param2=self._destination,
            )
            event.add(event=params)


# %%
