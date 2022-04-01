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
# Modified : Thursday, March 31st 2022, 3:38:39 pm                                                 #
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
from pyspark.sql.types import StringType, DoubleType, StructField, StructType, LongType
import pandas as pd
import numpy as np
from typing import Any

from deepcvr.base.operator import Operator
from deepcvr.utils.decorators import operator

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
MAX_PARTITION_SIZE = 1024 * 1024 * 100
# ------------------------------------------------------------------------------------------------ #
#                                  COLUMN LABELER                                                  #
# ------------------------------------------------------------------------------------------------ #


class ColumnLabeler(Operator):
    """Adds column names to a DataFrame. Includes support for threaded concurrency."""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(ColumnLabeler, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> Any:
        """Executes the labeling task

        Args:
            data (pd.DataFrame): Data from the previous step
            context (dict): Context data shared by operators within a dag
        """
        data.columns = self._params["names"]
        return data


# ------------------------------------------------------------------------------------------------ #
#                                  TARGET LABELER                                                  #
# ------------------------------------------------------------------------------------------------ #


class TargetLabeler(Operator):
    """Adds the target labels for view, clicks, and conversions."""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(TargetLabeler, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> Any:
        """Executes the preprocessing task

        Adds a target label variable

        Args:
            data (pd.DataFrame): Data from the previous step
            context (dict): Context data shared by operators within a dag
        """
        return self._add_label(data=data)

    def _add_label(self, data: pd.DataFrame) -> pd.DataFrame:
        """Adds the appropriate label, either 'View', 'Click', and 'Convert' to core dataset"""

        data["target_label"] = np.where(data["conversion_label"] == 1, "Convert", "View")
        data["target_label"] = np.where(data["click_label"] == 1, "Click", "View")

        return data


# ------------------------------------------------------------------------------------------------ #
#                                    DATA TYPER                                                    #
# ------------------------------------------------------------------------------------------------ #
class DataTyper(Operator):
    """Casts the datatypes in a DataFrame. Includes support for threaded concurrency."""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(DataTyper, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> Any:
        """Executes the data typing task

        Args:
            data (pd.DataFrame): Data from the previous step
            context (dict): Context data shared by operators within a dag
        """
        for k, v in self._params["dtype"].items():
            data[k].astype(v, errors="ignore")
        return data


# ------------------------------------------------------------------------------------------------ #
#                                    DATA SELECTOR                                                 #
# ------------------------------------------------------------------------------------------------ #
class DataSelector(Operator):
    """Reads and returns selected columns from the input DataFrame ."""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(DataSelector, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> Any:
        """Executes the data typing task

        Args:
            data (pd.DataFrame): Data from the previous step
            context (dict): Context data shared by operators within a dag
        """
        return data[self._params["names"]]


# ------------------------------------------------------------------------------------------------ #
#                                FEATURE TRANSFORMER                                               #
# ------------------------------------------------------------------------------------------------ #
class FeatureTransformer(Operator):
    """Extracts, reformats and stores core features in a format for loading. """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(FeatureTransformer, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> None:
        """Core feature extraction and conversion

        Args:
            data (pd.DataFrame): Context data for the DAG
            context (dict): Context data shared by operators within a dag
        """
        logger.info(data.head())
        data = self._partition_data(data, self._params["n_partitions"])

        cores = str("local[" + str(self._params["cores"]) + "]")
        spark = (
            SparkSession.builder.master(cores).appName("DeepCVR Core Features ETL").getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

        sdf = spark.createDataFrame(data)
        if self._is_sample_feature_data(data):
            logger.info("Sample data")
            logger.info(data.head())
            result = sdf.groupby("partition").apply(sample_features)
        else:
            logger.info("Not sample data")
            logger.info(data.head())
            result = sdf.groupby("partition").apply(common_features)
        df = result.toPandas()

        return df

    def _is_sample_feature_data(self, data: pd.DataFrame) -> bool:
        return "sample_id" in data.columns

    def _partition_data(self, data: pd.DataFrame, n_partitions: int) -> pd.DataFrame:
        """Partitions the dataset"""

        reindexed = data.reset_index()
        data["partition"] = reindexed.index % n_partitions
        return data


# ------------------------------------------------------------------------------------------------ #
sample_schema = StructType(
    [
        StructField("sample_id", LongType(), False),
        StructField("feature_name", StringType(), False),
        StructField("feature_id", LongType(), False),
        StructField("feature_value", DoubleType(), False),
    ]
)


# ------------------------------------------------------------------------------------------------ #
@pandas_udf(sample_schema, PandasUDFType.GROUPED_MAP)
def sample_features(partition):

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
