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
# Modified : Thursday, March 10th 2022, 12:02:44 am                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Transforms ALI-CCP impression and feature data into 3rd Normal Form prior to loading."""
import logging
import re
import inspect
import importlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import LongType, StringType, DoubleType, StructField, StructType
import pandas as pd
from typing import Union

from deepcvr.data.core import Task, ETLDag

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
class TransformCoreTask(Task):
    """Transforms a core dataset into an core sans the feature list."""

    def __init__(self, task_id: int, task_name: str, param: list) -> None:
        super(TransformCoreTask, self).__init__(task_id=task_id, task_name=task_name, param=param)

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transforms the Core data into an impressions file"""
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        columns = self._param
        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        return data[columns]


# ------------------------------------------------------------------------------------------------ #
class TransformCommonFeatureGroupsTask(Task):
    """Transforms a common features file to a common feature group file sans the features."""

    def __init__(self, task_id: int, task_name: str, param: list) -> None:
        super(TransformCommonFeatureGroupsTask, self).__init__(
            task_id=task_id, task_name=task_name, param=param
        )

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transforms the Common features data into a common feature group file"""
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        columns = self._param
        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        return data[columns]


# ------------------------------------------------------------------------------------------------ #
class TransformCoreFeaturesTask(Task):
    """Transforms core feature list into 3rd normal form """

    def __init__(self, task_id: int, task_name: str, param: list) -> None:
        super(TransformCoreFeaturesTask, self).__init__(
            task_id=task_id, task_name=task_name, param=param
        )

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transforms core feature list into 3rd normal form

        Args
            data (pd.DataFrame): Input data. Optional
        """
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        spark = (
            SparkSession.builder.master("local[24]")
            .appName("DeepCVR Core Features ETL")
            .getOrCreate()
        )

        sdf = spark.createDataFrame(data)

        result = sdf.groupby("partition").apply(transform_core_features)

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        return result.toPandas()


# ------------------------------------------------------------------------------------------------ #
class TransformCommonFeaturesTask(Task):
    """Transforms core feature list into 3rd normal form """

    def __init__(self, task_id: int, task_name: str, param: list) -> None:
        super(TransformCommonFeaturesTask, self).__init__(
            task_id=task_id, task_name=task_name, param=param
        )

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transforms common feature list into 3rd normal form

        Args
            data (pd.DataFrame): Input data. Optional
        """
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        spark = (
            SparkSession.builder.master("local[24]")
            .appName("DeepCVR Common Features ETL")
            .getOrCreate()
        )

        sdf = spark.createDataFrame(data)

        result = sdf.groupby("partition").apply(transform_common_features)

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        return result.toPandas()


# ------------------------------------------------------------------------------------------------ #
schema_core_features = StructType(
    [
        StructField("sample_id", LongType(), False),
        StructField("feature_name", StringType(), False),
        StructField("feature_id", LongType(), False),
        StructField("feature_value", DoubleType(), False),
    ]
)
# ------------------------------------------------------------------------------------------------ #


@pandas_udf(schema_core_features, PandasUDFType.GROUPED_MAP)
def transform_core_features(partition):

    logging.debug(
        "\tTransforming core features partition of {} observations.".format(str(partition.shape[0]))
    )

    logger.info(40 * "=")
    logger.info(partition.head())

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
schema_common_features = StructType(
    [
        StructField("common_features_index", StringType(), False),
        StructField("feature_name", StringType(), False),
        StructField("feature_id", LongType(), False),
        StructField("feature_value", DoubleType(), False),
    ]
)
# ------------------------------------------------------------------------------------------------ #


@pandas_udf(schema_common_features, PandasUDFType.GROUPED_MAP)
def transform_common_features(partition):

    logging.debug(
        "\tTransforming common features partition of {} observations.".format(
            str(partition.shape[0])
        )
    )

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
    logger.debug(feature_string)
    feature_structures = re.split("\x01", feature_string)

    for structure in feature_structures:
        name, id, value = re.split("\x02|\x03", structure)
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
    assert df.shape[0] == num_features, logging.error(
        "Number of features mismatch index {}. Expected: {}. Actual: {}.".format(
            id_value, num_features, df.shape[0]
        )
    )
    return df


# ------------------------------------------------------------------------------------------------ #


class TransformDAG(ETLDag):
    """Directed acyclic graph for the transform phase of the ETL

    Args:
        param_filepath (str): The location of the

    """

    def __init__(self, dag_id: dict) -> None:
        super(TransformDAG, self).__init__(dag_id=dag_id)


# ------------------------------------------------------------------------------------------------ #


class TransformDAGGenerator:
    """Generates a series of transformation DAGS

    Args:
        config (dict): Configurations for multiple dags

    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._dags = []

    @property
    def dags(self) -> dict:
        return self._dags

    def execute(self) -> None:
        """Iterates through the param and generates the DAGS"""

        for dag_id, tasks in self._config.items():
            dag = TransformDAG(dag_id=dag_id)
            for _, task_config in tasks.items():

                # Create task object from string using importlib

                module = importlib.import_module(task_config["module"])
                task = getattr(module, task_config["task"])

                task_instance = task(
                    task_id=task_config["task_id"],
                    task_name=task_config["task_name"],
                    param=task_config["task_param"],
                )

                dag.add_task(task_instance)
            self._dags.append(dag)

    def print(self) -> None:
        for dag in self._dags:
            logger.info(dag.print_tasks())
