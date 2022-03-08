#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /download.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, February 27th 2022, 8:31:48 am                                                #
# Modified : Sunday, February 27th 2022, 4:21:08 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Transform Operator. Encapsulates Transformer objects under the Airflow framework."""
import os
from deepcvr.data.transform import Transformer, Worker, Reducer
from airflow.models.baseoperator import BaseOperator
from deepcvr.utils.io import save_csv

# ------------------------------------------------------------------------------------------------ #


class TransformOperator(BaseOperator):
    """An Airflow wrapper for a data transformation object.

    It is parameterized by a transform object that conforms to the Transformer interface.
    This operator encapsulates transformers that read input data, perform processing and
    writes output data to file.

    Args:
        transformer (Transformer): Transformer class object, uninstantiated.
        source (str): The filepath to the input data
        destination (str): The filepath of the output data.
        force (bool): If True, the transformation is executed and any existing data are overwritten.
    """

    def __init__(
        self, transformer: Transformer, source: str, destination: str, force: bool = False, **kwargs
    ) -> None:
        super(TransformOperator, self).__init__(**kwargs)

        self._transformer = transformer(source=source, destination=destination, force=force)

    def execute(self, context) -> None:
        self._transformer.execute()


# ------------------------------------------------------------------------------------------------ #
class ProcessOperator(BaseOperator):
    """An Airflow Operator for Data Processing Tasks

    It is parameterized by a worker object that conforms to the Worker interface, performs
    processing on input data and returns the output to this operator.

    Args:
        worker (Worker): Worker object, instantiated with input data.
        source (str): The filepath to the input data
        destination (str): The filepath to output data.
    """

    def __init__(self, worker: Worker, source: str, destination: str, **kwargs) -> None:
        super(ProcessOperator, self).__init__(**kwargs)
        self._worker = worker
        self._source = source
        self._destination = destination

    def execute(self, context) -> None:
        output = self._worker.execute()

        # These are parallelized. The output filenames will contain the task id to
        # distinguish the worker that produced it.
        filename = self.task_id + ".csv"
        filepath = os.path.join(self._destination, filename)

        # Yeah, totally random.
        chunksize = int(output.shape[0] / 20)

        save_csv(data=output, filepath=filepath, header=True, index=False, chunksize=chunksize)


# ------------------------------------------------------------------------------------------------ #
class ReductionOperator(BaseOperator):
    """An Airflow Operator that performs reduction operations in a parallel computing context.

    It is parameterized by a reducer object which takes an input directory containing output
    of various tasks and combines the output into a single data set at the destination.

    Args:
        reducer (Reducer): Reducer class uninstantiated
        source (str): The directory to the input data
        destination (str): The filepath to output data.
    """

    def __init__(self, reducer: Reducer, source: str, destination: str, **kwargs) -> None:
        super(ReductionOperator, self).__init__(**kwargs)

        self._source = source
        self._destination = destination
        self._reducer = reducer(source=self._source, destination=destination)

    def execute(self, context) -> None:
        self._reducer.execute()
