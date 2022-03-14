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
# Modified : Sunday, March 13th 2022, 2:10:15 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Base and abstract class definitions."""
from abc import ABC, abstractmethod
import importlib
from typing import Any
import logging

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
logcvr = logging.getLogger("deepcvr")
# ------------------------------------------------------------------------------------------------ #


class Task(ABC):
    """Abstract class for task classes

    Args:
        task_id: Sequence number for the task in its dag
        task_name: Human readable name
        params: The parameters the task requires

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


class Dag:
    """Directed acyclic graph of tasks.

    Args:
        dag_id (str): Identifier for the dag
        dag_description (str): Brief description
        tasks (list): List of tasks to execute

    """

    def __init__(self, dag_id: str, dag_description: str, tasks: list, context: Any = None) -> None:
        self._dag_id = dag_id
        self._dag_description = dag_description
        self._tasks = tasks
        self._context = context

    def run(self) -> None:
        for task in self._tasks:
            logger.info("Task {}: {} started.".format(str(task.task_id), task.task_name))
            task.execute(context=self._context)
            logger.info("Task {}: {} completed.".format(str(task.task_id), task.task_name))

    def print_tasks(self) -> None:
        for task in self._tasks:
            logger.info(task.task_id)
            logger.info(task.task_name)
            logger.info(task.params)
            logger.info("\n")


# ------------------------------------------------------------------------------------------------ #


class DagBuilder:
    """Constructs a DAG from a configuration dictionary

    Args:
        config (dict): Nested dictionary of tasks defined by a dag_id, dag_description and
        a nested dictionary of tasks, where each task is defined by:
          task_id: Sequence number of task
          task: Name of the class that executes the task
          module: The module containing the task
          task_name: A name for the task
          task_params: Any parameters required by the task
        mode (str): 'd' for development or 'p' for production.
    """

    def __init__(self, config: dict, mode: str = "d", context: dict = None) -> None:
        self._config = config
        self._mode = "production" if mode == "p" else "development"
        self._context = context
        self._dag = None

    @property
    def dag(self) -> Dag:
        return self._dag

    def build(self) -> Dag:

        config = self._config[self._mode]

        dag_id = config["dag_id"]
        dag_description = config["dag_description"]

        tasks = []

        for _, task_config in config["tasks"].items():

            # Create task object from string using importlib

            module = importlib.import_module(task_config["module"])
            task = getattr(module, task_config["task"])

            task_instance = task(
                task_id=task_config["task_id"],
                task_name=task_config["task_name"],
                params=task_config["task_params"],
            )

            logger.debug(task_instance)

            tasks.append(task_instance)

        self._dag = Dag(
            dag_id=dag_id, dag_description=dag_description, tasks=tasks, context=self._context
        )

        return self._dag
