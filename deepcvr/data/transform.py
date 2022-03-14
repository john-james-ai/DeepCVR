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
# Modified : Sunday, March 13th 2022, 5:36:02 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Transforms ALI-CCP impression and feature data into 3rd Normal Form prior to loading."""
import logging
import re
import inspect
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import LongType, StringType, DoubleType, StructField, StructType
import pandas as pd
from typing import Union, Any

from deepcvr.data.core import Task
from deepcvr.utils.io import CsvIO
from deepcvr.data.metabase import Asset, Event, EventParams

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
#                                    TRANSFORM CORE                                                #
# ------------------------------------------------------------------------------------------------ #


class TransformImpressionsTask(Task):
    """Transforms a core dataset into an core sans the feature list."""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(TransformImpressionsTask, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )

    def execute(self, context: Any = None) -> None:
        """Transforms core dataset removing features and restoring normal form.

        Args
            data (pd.DataFrame): Input data. Optional
        """
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        io = CsvIO()
        data = io.load(self._params["input_filepath"])

        spark = (
            SparkSession.builder.master("local[24]")
            .appName("DeepCVR Core Features ETL")
            .getOrCreate()
        )

        sdf = spark.createDataFrame(data)

        result = sdf.groupby("partition").apply(transform_core)

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        pdf = result.toPandas()

        io.save(pdf, self._params["output_filepath"])

    def _register_event(self) -> None:
        """Registers the event with metadata"""
        # Load asset and event tables
        asset = Asset()
        asset.add(name="impressions", desc="impressions table", uri=self._params["output_filepath"])
        event = Event()
        params = EventParams(
            module=__name__,
            classname=__class__.__name__,
            method="execute",
            action="transform_impressions",
            param1=self._params["input_filepath"],
            param2=self._params["output_filepath"],
        )
        event.add(event=params)


# ------------------------------------------------------------------------------------------------ #
schema_core = StructType(
    [
        StructField("sample_id", DoubleType(), False),
        StructField("click_label", DoubleType(), False),
        StructField("conversion_label", DoubleType(), False),
        StructField("common_features_index", StringType(), False),
        StructField("num_features", DoubleType(), False),
        StructField("partition", DoubleType(), False),
    ]
)
# ------------------------------------------------------------------------------------------------ #


@pandas_udf(schema_core, PandasUDFType.GROUPED_MAP)
def transform_core(partition):

    logger.debug(
        "\tTransforming core partition of {} observations.".format(str(partition.shape[0]))
    )

    output = partition[
        [
            "sample_id",
            "click_label",
            "conversion_label",
            "common_features_index",
            "num_features",
            "partition",
        ]
    ]

    return output


# ------------------------------------------------------------------------------------------------ #
#                          TRANSFORM COMMON FEATURE GROUPS                                         #
# ------------------------------------------------------------------------------------------------ #


class TransformCommonFeatureGroupsTask(Task):
    """Transforms common features dataset into a common feature group dataset ."""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(TransformCommonFeatureGroupsTask, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )

    def execute(self, context: Any = None) -> None:
        """Transforms common features dataset to common feature groups by removing features and
        restoring normal form.

        Args
            data (pd.DataFrame): Input data. Optional
        """
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        io = CsvIO()
        data = io.load(self._params["input_filepath"])

        spark = (
            SparkSession.builder.master("local[24]")
            .appName("DeepCVR Core Features ETL")
            .getOrCreate()
        )

        sdf = spark.createDataFrame(data)

        result = sdf.groupby("partition").apply(transform_common_feature_groups)

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        pdf = result.toPandas()

        io.save(pdf, self._params["output_filepath"])

    def _register_event(self) -> None:
        """Registers the event with metadata"""
        # Load asset and event tables
        asset = Asset()
        asset.add(
            name="common_feature_groups",
            desc="feature groups table",
            uri=self._params["output_filepath"],
        )
        event = Event()
        params = EventParams(
            module=__name__,
            classname=__class__.__name__,
            method="execute",
            action="transform_common_feature_groups",
            param1=self._params["input_filepath"],
            param2=self._params["output_filepath"],
        )
        event.add(event=params)


# ------------------------------------------------------------------------------------------------ #
schema_common_feature_groups = StructType(
    [
        StructField("common_features_index", StringType(), False),
        StructField("num_features", DoubleType(), False),
        StructField("partition", DoubleType(), False),
    ]
)
# ------------------------------------------------------------------------------------------------ #


@pandas_udf(schema_common_feature_groups, PandasUDFType.GROUPED_MAP)
def transform_common_feature_groups(partition):

    logger.debug(
        "\tTransforming core partition of {} observations.".format(str(partition.shape[0]))
    )

    output = partition[["common_features_index", "num_features", "partition"]]

    return output


# ------------------------------------------------------------------------------------------------ #
#                                TRANSFORM CORE FEATURES                                           #
# ------------------------------------------------------------------------------------------------ #
class TransformFeaturesTask(Task):
    """Transforms core feature list into 3rd normal form """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(TransformFeaturesTask, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )

    def execute(self, context: Any = None) -> None:
        """Transforms core feature list into 3rd normal form

        Args
            data (pd.DataFrame): Input data. Optional
        """
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        io = CsvIO()
        data = io.load(self._params["input_filepath"])

        spark = (
            SparkSession.builder.master("local[24]")
            .appName("DeepCVR Core Features ETL")
            .getOrCreate()
        )

        sdf = spark.createDataFrame(data)

        result = sdf.groupby("partition").apply(transform_core_features)

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        pdf = result.toPandas()

        io.save(pdf, self._params["output_filepath"])

    def _register_event(self) -> None:
        """Registers the event with metadata"""
        # Load asset and event tables
        asset = Asset()
        asset.add(name="features", desc="features table", uri=self._params["output_filepath"])
        event = Event()
        params = EventParams(
            module=__name__,
            classname=__class__.__name__,
            method="execute",
            action="transform_features",
            param1=self._params["input_filepath"],
            param2=self._params["output_filepath"],
        )
        event.add(event=params)


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

    logger.debug(
        "\tTransforming core features partition of {} observations.".format(str(partition.shape[0]))
    )

    output = pd.DataFrame()

    for _, row in partition.iterrows():
        sample_id = int(row[0])
        num_features = int(row[4])
        feature_string = row[5]

        df = parse_feature_string(
            id_name="sample_id",
            id_value=sample_id,
            num_features=num_features,
            feature_string=feature_string,
        )

        output = pd.concat([output, df], axis=0)

    return output


# ------------------------------------------------------------------------------------------------ #
#                               TRANSFORM COMMON FEATURES                                          #
# ------------------------------------------------------------------------------------------------ #
class TransformCommonFeaturesTask(Task):
    """Transforms core feature list into 3rd normal form """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(TransformCommonFeaturesTask, self).__init__(
            task_id=task_id, task_name=task_name, params=params
        )

    def execute(self, context: Any = None) -> None:
        """Transforms common feature list into 3rd normal form"""
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        io = CsvIO()
        data = io.load(self._params["input_filepath"])

        spark = (
            SparkSession.builder.master("local[24]")
            .appName("DeepCVR Common Features ETL")
            .getOrCreate()
        )

        sdf = spark.createDataFrame(data)

        result = sdf.groupby("partition").apply(transform_common_features)

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        pdf = result.toPandas()

        io.save(pdf, self._params["output_filepath"])

    def _register_event(self) -> None:
        """Registers the event with metadata"""
        # Load asset and event tables
        asset = Asset()
        asset.add(
            name="common_features",
            desc="common features table",
            uri=self._params["output_filepath"],
        )
        event = Event()
        params = EventParams(
            module=__name__,
            classname=__class__.__name__,
            method="execute",
            action="transform_common_features",
            param1=self._params["input_filepath"],
            param2=self._params["output_filepath"],
        )
        event.add(event=params)


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

    logger.debug(
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
    assert df.shape[0] == num_features, logger.error(
        "Number of features mismatch index {}. Expected: {}. Actual: {}.".format(
            id_value, num_features, df.shape[0]
        )
    )
    return df
