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
# Modified : Monday, March 21st 2022, 12:54:35 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Transforms ALI-CCP impression and feature data into 3rd Normal Form prior to loading."""
import re
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructField, StructType
import pandas as pd
from typing import Union, Any

from deepcvr.base.task import Task
from deepcvr.utils.io import CsvIO
from deepcvr.utils.decorators import task_event

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                      SPLIT MERGE                                                 #
# ------------------------------------------------------------------------------------------------ #


class CoreFeatureConversion(Task):
    """Extracts, reformats and stores core features in a format for loading. """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(CoreFeatureConversion, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )

    @task_event
    def execute(self, context: Any = None) -> None:
        """Core feature extraction and conversion

        Args:
            context (dict): Context data for the DAG
        """
        # ---------------------------------------------------------------------------------------- #
        # Load, partition, and create the core dataset

        logger.debug("Reading core dataset.")
        io = CsvIO()
        data = io.load(self._params["input_filepath"])

        logger.debug("Partitioning core dataset")
        reindexed = data.reset_index()
        data["partition"] = reindexed.index % self._params["n_partitions"]

        logger.debug("Create Spark context")
        spark = (
            SparkSession.builder.master("local[20]")
            .appName("DeepCVR Core Features ETL")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        logger.debug("Create Spark dataframe and dispatch the parallelism")
        sdf = spark.createDataFrame(data)
        result = sdf.groupby("partition").apply(core_features)
        df = result.toPandas()

        # Save the core dataset
        io.save(
            df,
            filepath=self._params["output_filepath"],
            engine="auto",
            partition_cols=["sample_id", "common_features_index"],
        )
        logger.debug("Collect the result and save the dataframe")


# ------------------------------------------------------------------------------------------------ #
class CommonFeatureConversion(Task):
    """Extracts, reformats and stores common features in a format for loading. """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(CommonFeatureConversion, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )

    @task_event
    def execute(self, context: Any = None) -> None:
        """Common feature extraction and conversion

        Args:
            context (dict): Context data for the DAG
        """
        # ---------------------------------------------------------------------------------------- #
        # Load, partition, and create the core dataset

        logger.debug("Reading core dataset.")
        io = CsvIO()
        data = io.load(self._params["input_filepath"])

        logger.debug("Partitioning core dataset")
        reindexed = data.reset_index()
        data["partition"] = reindexed.index % self._params["n_partitions"]

        logger.debug("Create Spark context")
        spark = (
            SparkSession.builder.master("local[20]")
            .appName("DeepCVR Core Features ETL")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        logger.debug("Create Spark dataframe and dispatch the parallelism")
        sdf = spark.createDataFrame(data)
        result = sdf.groupby("partition").apply(common_features)

        df = result.toPandas()

        # Save the core dataset
        io.save(
            df,
            filepath=self._params["output_filepath"],
            engine="auto",
            partition_cols=["common_features_index"],
        )
        logger.debug("Collect the result and save the dataframe")


# ------------------------------------------------------------------------------------------------ #
#                               CORE FEATURE SCHEMA AND UDF                                        #
# ------------------------------------------------------------------------------------------------ #
core_schema = StructType(
    [
        StructField("sample_id", DoubleType(), False),
        StructField("num_features", DoubleType(), False),
        StructField("features_list", StringType(), False),
        StructField("partition", IntegerType(), False),
    ]
)
# ------------------------------------------------------------------------------------------------ #


@pandas_udf(core_schema, PandasUDFType.GROUPED_MAP)
def core_features(partition):

    output = pd.DataFrame()

    for _, row in partition.iterrows():
        print(row)
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
#                              COMMON FEATURE SCHEMA AND UDF                                       #
# ------------------------------------------------------------------------------------------------ #
common_schema = StructType(
    [
        StructField("common_features_index", StringType(), False),
        StructField("num_features", StringType(), False),
        StructField("features_list", StringType(), False),
        StructField("partition", IntegerType(), False),
    ]
)

# ------------------------------------------------------------------------------------------------ #


@pandas_udf(core_schema, PandasUDFType.GROUPED_MAP)
def common_features(partition):

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


def parse_feature_string(
    id_name: str, id_value: Union[str, int], num_features: int, feature_string: str
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
    assert df.shape[0] == num_features, print("Feature count doesn't not match num_features")
    return df
