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
# Modified : Monday, March 21st 2022, 4:09:53 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import os
import re
import boto3
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, LongType, DoubleType
import inspect
import progressbar
import tarfile
from botocore.exceptions import NoCredentialsError
from typing import Any
from dotenv import load_dotenv

from deepcvr.base.task import Task
from deepcvr.utils.decorators import task_event
from deepcvr.utils.io import CsvIO

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
MAX_PARTITION_SIZE = 1024 * 1024


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
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
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
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
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

        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

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
        super(Stage, self).__init__(task_id=task_id, task_name=task_name, params=params)

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

        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

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
            io = CsvIO()
            io.save(df, filepath=destination)


# ------------------------------------------------------------------------------------------------ #
class FeatureExtractor(Task):
    """Base class for Feature Extractors"""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(FeatureExtractor, self).__init__(task_id=task_id, task_name=task_name, params=params)
        self._input_filepath = self._params["input_filepath"]
        self._output_filepath = self._params["output_filepath"]
        self._cores = self._params["cores"]
        self._columns = self._params["columns"]

        self._io = CsvIO()

    def _load_partitioned_data(self) -> pd.DataFrame:
        """Loads and adds a partition variable to the dataframe"""

        data = self._io.load(self._input_filepath, usecols=self._columns)
        size_in_memory = data.memory_usage(deep=True).sum()
        num_partitions = int(size_in_memory / MAX_PARTITION_SIZE)
        num_partitions_setting = max(180, num_partitions)
        reindexed = data.reset_index()
        data["partition"] = reindexed.index % num_partitions_setting
        return data


# ------------------------------------------------------------------------------------------------ #
#                                      CORE FEATURES                                               #
# ------------------------------------------------------------------------------------------------ #
class CoreFeatureExtractor(FeatureExtractor):
    """Extracts, reformats and stores core features in a format for loading. """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(CoreFeatureExtractor, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )

    @task_event
    def execute(self, context: Any = None) -> None:
        """Core feature extraction and conversion

        Args:
            context (dict): Context data for the DAG
        """

        logger.debug("\tReading and partition core dataset.")
        data = self._load_partitioned_data()

        logger.debug("\tCreate Spark context")
        cores = str("local[" + str(self._cores) + "]")
        spark = (
            SparkSession.builder.master(cores).appName("DeepCVR Core Features ETL").getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        logger.debug("\tCreating dataframe")
        sdf = spark.createDataFrame(data)
        logger.debug("\tProcessing Groupby")
        result = sdf.groupby("partition").apply(core_features)
        logger.debug("\tConverted to Pandas dataframe")
        df = result.toPandas()
        logger.debug(df.head())

        # Save the core dataset
        self._io.save(df, filepath=self._output_filepath)
        logger.debug("Collect the result and save the dataframe")


# ------------------------------------------------------------------------------------------------ #
core_schema = StructType(
    [
        StructField("sample_id", LongType(), False),
        StructField("feature_name", StringType(), False),
        StructField("feature_id", LongType(), False),
        StructField("feature_value", DoubleType(), False),
    ]
)


# ------------------------------------------------------------------------------------------------ #
@pandas_udf(core_schema, PandasUDFType.GROUPED_MAP)
def core_features(partition):

    output = pd.DataFrame()

    for _, row in partition.iterrows():
        sample_id = int(row[0])
        num_features = int(row[1])
        feature_string = row[2]

        df = parse_feature_string(
            id_name="sample_id",
            id_value=sample_id,
            num_features=num_features,
            feature_string=feature_string,
        )

        output = pd.concat([output, df], axis=0)

    return output


# ------------------------------------------------------------------------------------------------ #
#                                     COMMON FEATURES                                              #
# ------------------------------------------------------------------------------------------------ #
class CommonFeatureExtractor(FeatureExtractor):
    """Extracts, reformats and stores common features in a format for loading. """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(CommonFeatureExtractor, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )

    @task_event
    def execute(self, context: Any = None) -> None:
        """Core feature extraction and conversion

        Args:
            context (dict): Context data for the DAG
        """

        logger.debug("\tReading and partition core dataset.")
        data = self._load_partitioned_data()

        logger.debug("\tCreate Spark context")
        cores = str("local[" + str(self._cores) + "]")
        spark = (
            SparkSession.builder.master(cores).appName("DeepCVR Core Features ETL").getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        logger.debug("\tCreate Spark dataframe")
        sdf = spark.createDataFrame(data)
        logger.debug("\tDataFrame created")
        result = sdf.groupby("partition").apply(common_features)
        logger.debug("\tResults Produced")
        df = result.toPandas()
        logger.debug("\tConverted to Pandas")

        # Save the core dataset
        self._io.save(df, filepath=self._output_filepath)
        logger.debug("\tCollect the result and save the dataframe")


# ------------------------------------------------------------------------------------------------ #
common_schema = StructType(
    [
        StructField("common_features_index", StringType(), False),
        StructField("feature_name", StringType(), False),
        StructField("feature_id", LongType(), False),
        StructField("feature_value", DoubleType(), False),
    ]
)

# ------------------------------------------------------------------------------------------------ #


@pandas_udf(common_schema, PandasUDFType.GROUPED_MAP)
def common_features(partition):

    output = pd.DataFrame()

    for _, row in partition.iterrows():
        common_features_index = row[0]
        num_features = int(row[1])
        feature_string = row[2]

        df = parse_feature_string(
            id_name="common_features_index",
            id_value=common_features_index,
            num_features=num_features,
            feature_string=feature_string,
        )

        output = pd.concat([output, df], axis=0)

    return output


# ------------------------------------------------------------------------------------------------ #


def parse_feature_string(
    id_name: str, id_value: Any, num_features: int, feature_string: str
) -> dict:
    """Parses a feature string from a single observation in the dataset.

    Feature strings contain one or more feature structures, each deliminated by ASCII character
    '0x01'. Each feature structure contains three components,
     - feature_name (string),
     - feature_id (int), and
     - feature_value (float)

    This function parses the feature string and its feature structures into three column DataFrame
    comprised of one row per feature structure.

    Args:
        id_name (str): The column name for the id
        id_value (str,int): The column value for the id
        num_features (int): The number of feature structures in the list
        feature_string (str): String containing feature structures

    """
    feature_names = []
    feature_ids = []
    feature_values = []

    # Expand into a list of feature structures
    feature_structures = re.split("\x01", str(feature_string))

    for structure in feature_structures:
        name, id, value = re.split("\x02|\x03", str(structure))
        feature_names.append(name)
        feature_ids.append(int(id))
        feature_values.append(float(value))

    d = {
        id_name: id_value,
        "feature_name": feature_names,
        "feature_id": feature_ids,
        "feature_value": feature_values,
    }
    df = pd.DataFrame(data=d)

    # Confirms number of rows equals expected number of features.
    assert df.shape[0] == num_features, logger.error("Feature count doesn't not match num_features")
    return df
