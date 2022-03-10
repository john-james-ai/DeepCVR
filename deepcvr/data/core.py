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
# Modified : Thursday, March 10th 2022, 3:34:33 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Base and abstract class definitions."""
from abc import ABC, abstractmethod
from typing import Any
import logging

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Task(ABC):
    """Abstract class for task classes

    Args:
        config_filepath (str): The location of the

    """

    def __init__(self, task_id: int, task_name: str, params: Any) -> None:
        self._task_id = task_id
        self._task_name = task_name
        self._params = params

    def __str__(self) -> str:
        return str(
            "Task id: {}\tTask name: {}\tParams: {}".format(
                self._task_id, self._task_name, self._params
            )
        )

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
    def execute(self) -> Any:
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

    def add_task(self, task: Task) -> None:
        """Adds a task to the DAG

        Args:
            task (Task): Class. One of the three supported classes
        """
        self._tasks.append(task)

    def run(self) -> None:
        for task in self._tasks:
            logger.info(task.task_name)
            task.execute()

    def print_tasks(self) -> None:
        for task in self._tasks:
            logger.info(task.task_id)
            logger.info(task.task_name)
            logger.info(task.params)
            logger.info("\n")
