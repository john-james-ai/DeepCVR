#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /distributed_processing.py                                                            #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/deepcvr                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, March 20th 2022, 1:34:43 am                                                   #
# Modified : Monday, March 21st 2022, 2:06:06 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Interfaces used to abstract external data processing frameworks such as Spark """
from abc import abstractmethod
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType

from deepcvr.utils.io import CsvIO
from deepcvr.base.task import Task

# ------------------------------------------------------------------------------------------------ #


class SparkPandasUDF(Task):
    """Abstracts the Pandas UDF capability within the Spark framework

    Spark Pandas UDF classes provide distributed processing for large pandas dataframes using the
    split-apply-combine pattern. At a high-level, Spark'S Pandas UDF facility first splits converts
    the pandas DataFrame to a Spark DataFrame, then the Spark DataFrame is split into groups based
    on the conditions specified in the groupby operator. It then dispatches the partition and
    the user-defined function (pandas.DataFrame -> pandas.DataFrame) to a cluster or CPU core (in
    standalone mode) for processing. The results are combined and returned as a new Spark DataFrame.

    This base class extends the Task interface. Subclasses must provide the implementation
    details including:
    - schema: a Spark StructType defining the StructFields. StructFields specify the variable
        names, data types and nullability of a row from the partitioned input.
    - _process_partition method containing the implementation of a user defined function.

    Args:
        task_id (int): A number, typically used to indicate the sequence of the task within a DAG
        task_name (str): String name
        params (Any): Parameters for the task including:
            input_filepath (str): The filepath containing input data
            output_filepath (str): Path for output
            cores (int): Number of cores to apply to this object.

    """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(SparkPandasUDF, self).__init__(task_id=task_id, task_name=task_name, params=params)
        self._input_filepath = self._params["input_filepath"]
        self._output_filepath = self._params["output_filepath"]
        self._cores = self._params["cores"]
        self._columns = self._params["columns"]

        self._result = None
        self._max_partition_size = 1024 * 1024 * 100
        self._io = CsvIO()

    @property
    @abstractmethod
    def schema(self) -> StructType:
        pass

    @property
    def result(self) -> pd.DataFrame:
        return self._result

    def execute(self) -> DataFrame:
        """Executes the defined Spark Pandas UDF on the data"""
        global schema
        schema = self.schema
        data = self._io.load(self._input_filepath, usecols=self._columns)
        data = self._partition_data(data)
        result = self._process_data(data)
        self._io.save(result, filepath=self._output_filepath)

    def _partition_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Partitions dataframe according to the size of partition output."""
        num_partitions = int(
            data.memory_usage(deep=True).sum() / self._cores / self._max_partition_size
        )
        reindexed = data.reset_index()
        data["partition"] = reindexed.index % num_partitions
        return data

    def _process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Orchestrates data processing via Spark context"""
        cores = str("local[" + str(self._cores) + "]")
        spark = SparkSession.builder.master(cores).appName("DeepCVR").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        sdf = spark.createDataFrame(data)
        result = sdf.groupby("partition").apply(self._process_partition)
        df = result.toPandas()

        return df

    @abstractmethod
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def _process_partition(self, partition: pd.DataFrame) -> pd.DataFrame:
        """User defined function to be performed on each partition"""
        pass

    def _create_spark_session(self) -> SparkSession:
        appname = "DeepCVR: " + self._task_name
        spark = SparkSession.builder.master(self._params["cores"]).appname(appname).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
