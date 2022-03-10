#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /core.py                                                                              #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, March 8th 2022, 8:48:19 pm                                                   #
# Modified : Wednesday, March 9th 2022, 11:46:42 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Base and abstract class definitions."""
from abc import ABC, abstractmethod
from typing import Any, Type
import pandas as pd
import logging

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Task(ABC):
    """Abstract class for task classes

    Args:
        config_filepath (str): The location of the

    """

    def __init__(self, task_id: int, task_name: str, param: Any) -> None:
        self._task_id = task_id
        self._task_name = task_name
        self._param = param

    @property
    def task_id(self) -> int:
        return self._task_id

    @property
    def task_name(self) -> str:
        return self._task_name

    @property
    def params(self) -> Any:
        return self._params

    @abstractmethod
    def execute(self, data: pd.DataFrame = None) -> Any:
        """Executes the task

        Args:
            data (pd.DataFrame): Input data. Optional

        """
        pass


# ------------------------------------------------------------------------------------------------ #


class ETLDag(ABC):
    """Abstract base class for ETL directed acyclic graphs

    Args:
        mode (str): Mode is 'dev' for development or 'prod' for production data.
        config_filepath (str): The location of the

    """

    def __init__(self, dag_id: dict) -> None:
        self._dag_id = dag_id
        self._tasks = []
        self._data = None

    def add_task(self, task: Type[Task]) -> None:
        """Adds a task to the DAG

        Args:
            task (Type[Task]): Class. One of the three supported classes
        """
        self._tasks.append(task)

    def run(self, data: pd.DataFrame = None) -> None:
        for task in self._tasks:
            logger.debug(task.task_name)
            data = task.execute(data=self._data)
            self._data = self._data if data is None else data

    def print_tasks(self) -> None:
        for task in self._tasks:
            logger.info(task.task_id)
            logger.info(task.task_name)
            logger.info(task.param)
            logger.info("\n")
