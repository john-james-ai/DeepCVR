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
# Modified : Wednesday, March 9th 2022, 5:30:34 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Transforms ALI-CCP impression and feature data into 3rd Normal Form prior to loading."""
import os
import logging
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import LongType, StringType, DoubleType, StructField, StructType
import pandas as pd
from typing import Union
from deepcvr.utils.io import load_csv, save_csv

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

schema_core_features = StructType(
    [
        StructField("sample_id", LongType(), False),
        StructField("feature_name", StringType(), False),
        StructField("feature_id", LongType(), False),
        StructField("feature_value", DoubleType(), False),
    ]
)
schema_common_features = StructType(
    [
        StructField("common_features_index", StringType(), False),
        StructField("feature_name", StringType(), False),
        StructField("feature_id", LongType(), False),
        StructField("feature_value", DoubleType(), False),
    ]
)

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
    assert df.shape[0] == num_features, logging.error(
        "Number of features mismatch index {}. Expected: {}. Actual: {}.".format(
            id_value, num_features, df.shape[0]
        )
    )
    return df


# ------------------------------------------------------------------------------------------------ #
@pandas_udf(schema_core_features, PandasUDFType.GROUPED_MAP)
def transform_core_features(partition):

    logging.debug(
        "\tTransforming core features partition of {} observations.".format(str(partition.shape[0]))
    )

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
def transform(in_filepath: str, out_filepath: str, filetype: str):
    """Orchestrates the transformation for a single file

    Args:
        in_filepath (str): Path to input file
        out_filepath (str): Path to output file
        filetype (str): Either "core" or "common" features file.

    """

    logging.debug("Started transformation of {}".format(in_filepath))

    spark = SparkSession.builder.master("local[24]").appName("DeepCVR ETL").getOrCreate()

    if "core" in filetype:
        df_in = load_csv(
            filepath=in_filepath,
            index_col=None,
            usecols=["sample_id", "num_core_features", "features_list", "partition"],
        )
        spark_df = spark.createDataFrame(df_in)
        df_out = spark_df.groupby("partition").apply(transform_core_features)
    else:
        df_in = load_csv(
            filepath=in_filepath,
            index_col=None,
            usecols=["common_features_index", "num_common_features", "features_list", "partition"],
        )
        spark_df = spark.createDataFrame(df_in)
        df_out = spark_df.groupby("partition").apply(transform_common_features)

    os.makedirs(os.path.dirname(out_filepath), exist_ok=True)

    df_out = df_out.toPandas()
    save_csv(data=df_out, filepath=out_filepath, sep=",", header=True, index=False)

    logging.debug("Completed transformation of {}".format(in_filepath))
