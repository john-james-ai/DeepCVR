#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /io.py                                                                                #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Wednesday, March 9th 2022, 2:18:40 pm                                                 #
# Modified : Wednesday, March 9th 2022, 11:53:29 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Read and Write Tasks"""
from abc import abstractmethod
import logging
import pandas as pd
from typing import Any
from deepcvr.data.core import Task
from deepcvr.utils.io import CsvIO

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Read(Task):
    """Base class for tasks that read data from file, database, etc...

    Args:
        mode (str): Mode is 'dev' for development or 'prod' for production data.
        config_filepath (str): The location of the

    """

    def __init__(self, task_id: int, task_name: str, param: Any) -> None:
        self._task_id = task_id
        self._task_name = task_name
        self._param = param

    @abstractmethod
    def execute(self, data: pd.DataFrame = None) -> Any:
        """Reads the data and returns a DataFrame
        Returns:
            pd.DataFrame

        """
        pass


# ------------------------------------------------------------------------------------------------ #


class ReadCSV(Read):
    """Reads from a CSV file

    Args:
        mode (str): Mode is 'dev' for development or 'prod' for production data.
        config_filepath (str): The location of the

    """

    def __init__(self, task_id: int, task_name: str, param: Any) -> None:
        super(ReadCSV, self).__init__(task_id=task_id, task_name=task_name, param=param)
        self._filepath = param

    def execute(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """Reads data from a CSV File"""

        io = CsvIO()
        data = io.load(self._filepath)

        logger.debug("\tRead DataFrame of shape {}".format(str(data.shape)))

        return data


# ------------------------------------------------------------------------------------------------ #


class Write(Task):
    """Base class for tasks that write data to file, database, etc...

    Args:
        mode (str): Mode is 'dev' for development or 'prod' for production data.
        config_filepath (str): The location of the

    """

    def __init__(self, task_id: int, task_name: str, param: Any) -> None:
        self._task_id = task_id
        self._task_name = task_name
        self._param = param

    @abstractmethod
    def execute(self, data: pd.DataFrame) -> Any:
        """Writes the data to the filepath designated in config.

        Args
            data (pd.DataFrame): Input data. Optional
        """
        pass


# ------------------------------------------------------------------------------------------------ #


class WriteCSV(Write):
    """Reads from a CSV file

    Args:
        mode (str): Mode is 'dev' for development or 'prod' for production data.
        config_filepath (str): The location of the

    """

    def __init__(self, task_id: int, task_name: str, param: Any) -> None:
        super(WriteCSV, self).__init__(task_id=task_id, task_name=task_name, param=param)

        self._filepath = param

    def execute(self, data: pd.DataFrame) -> Any:
        """Writes the data to the filepath designated in config.

        Args
            data (pd.DataFrame): Input data. Optional
        """
        io = CsvIO()
        io.save(data=data, filepath=self._filepath)

        logger.debug("\tSaved a DataFrame of shape {}".format(str(data.shape)))
