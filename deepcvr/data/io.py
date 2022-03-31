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
# URL      : https://github.com/john-james-ai/deepcvr                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, March 22nd 2022, 4:02:42 am                                                  #
# Modified : Thursday, March 31st 2022, 6:50:07 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from abc import ABC, abstractmethod
import pandas as pd
from typing import Any

from deepcvr.utils.io import CsvIO
from deepcvr.base.operator import Operator
from deepcvr.utils.decorators import task_event

# ------------------------------------------------------------------------------------------------ #
#                                           IO                                                     #
# ------------------------------------------------------------------------------------------------ #


class IO(Operator, ABC):
    """Abstract base class for IO related classes."""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(IO, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @abstractmethod
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                          CSV                                                     #
# ------------------------------------------------------------------------------------------------ #


class CSVReader(IO):
    """Read operator for DAGS"""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(CSVReader, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @task_event
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        io = CsvIO()
        if self._params.get("usecols", None):
            data = io.load(
                filepath=self._params["filepath"], header=0, usecols=self._params["usecols"],
            )
        else:
            data = io.load(filepath=self._params["filepath"], header=0)

        return data


# ------------------------------------------------------------------------------------------------ #


class CSVWriter(IO):
    """Write operator for DAGS"""

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(CSVWriter, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @task_event
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> pd.DataFrame:
        """Reads from the designated resource"""
        io = CsvIO()
        io.save(data=data, filepath=self._params["filepath"])
        return None
