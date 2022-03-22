#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /transform.py                                                                         #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, February 27th 2022, 10:11:02 am                                               #
# Modified : Monday, March 21st 2022, 10:16:18 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Transforms ALI-CCP impression and feature data into 3rd Normal Form prior to loading."""
import os
import re
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType, DoubleType, StructField, StructType, LongType
import pandas as pd
from typing import Any

from deepcvr.base.task import Task
from deepcvr.utils.io import CsvIO
from deepcvr.utils.decorators import task_event

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
MAX_PARTITION_SIZE = 1024 * 1024 * 100
# ------------------------------------------------------------------------------------------------ #
#                                     PREPROCESS                                                   #
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
        source = self._params["input_filepath"]
        destination = self._params["output_filepath"]
        force = self._params["force"]
        columns = self._params["columns"]
        dtypes = self._params["dtypes"]

        os.makedirs(os.path.dirname(destination), exist_ok=True)

        if not os.path.exists(destination) or force:

            io = CsvIO()
            df = io.load(source, header=None, names=columns, index_col=False, dtype=dtypes)
            io.save(df, filepath=destination)


# ------------------------------------------------------------------------------------------------ #
#                                         STAGE                                                    #
# ------------------------------------------------------------------------------------------------ #
class Stage(Task):
    """Stages the core dataset for the load phase."""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(Stage, self).__init__(task_id=task_id, task_name=task_name, params=params)

        self._io = CsvIO()

    @task_event
    def execute(self, context: Any = None) -> Any:
        """Executes the preprocessing task

        Adds column names and casts the data types

        Args:
            context (dict): Parameters passed from the pipeline
        """
        df = self._io.load(filepath=self._params["input_filepath"], usecols=self._params["columns"])
        self._io.save(data=df, filepath=self._params["output_filepath"])


# ------------------------------------------------------------------------------------------------ #
#                                         STAGE                                                    #
# ------------------------------------------------------------------------------------------------ #
class FeatureTransformer(Task):
    """Base class for Feature Extractors"""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(FeatureTransformer, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )
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
        num_partitions_setting = max(4 * self._cores, num_partitions)
        reindexed = data.reset_index()
        data["partition"] = reindexed.index % num_partitions_setting
        return data


# ------------------------------------------------------------------------------------------------ #
#                                      CORE FEATURES                                               #
# ------------------------------------------------------------------------------------------------ #
class CoreFeatureTransformer(FeatureTransformer):
    """Extracts, reformats and stores core features in a format for loading. """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(CoreFeatureTransformer, self).__init__(
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
        self._io.save(df, filepath=self._output_filepath, index=True, index_label="id")
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
class CommonFeatureTransformer(FeatureTransformer):
    """Extracts, reformats and stores common features in a format for loading. """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(CommonFeatureTransformer, self).__init__(
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
        self._io.save(df, filepath=self._output_filepath, index=True, index_label="id")
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
