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
# Modified : Thursday, March 31st 2022, 5:34:35 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Defines the interfaces for classes involved in the construction and implementation of DAGS."""
from abc import ABC, abstractmethod
import importlib
from typing import Any

# ------------------------------------------------------------------------------------------------ #


class AbstractDAG(ABC):
    """Abstract base class for directed acyclic graph of operations.

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

    @abstractmethod
    def run(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class Dag(AbstractDAG):
    """Directed acyclic graph of operations.

    Args:
        dag_id (str): Identifier for the dag
        dag_description (str): Brief description
        tasks (list): List of tasks to execute

    """

    def __init__(self, dag_id: str, dag_description: str, tasks: list, context: Any = None) -> None:
        super(Dag, self).__init__(
            dag_id=dag_id, dag_description=dag_description, tasks=tasks, context=context
        )

    def run(self) -> None:
        data = None
        for task in self._tasks:
            result = task.execute(data=data, context=self._context)
            data = result if result is not None else data


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
    """

    def __init__(self, config: dict, context: dict = None) -> None:
        self._config = config
        self._context = context
        self._dag = None

    @property
    def dag(self) -> Dag:
        return self._dag

    def build(self) -> Dag:

        dag_id = self._config["dag_id"]
        dag_description = self._config["dag_description"]

        tasks = []

        for _, task_config in self._config["tasks"].items():

            # Create task object from string using importlib

            module = importlib.import_module(task_config["module"])
            task = getattr(module, task_config["task"])

            task_instance = task(
                task_id=task_config["task_id"],
                task_name=task_config["task_name"],
                params=task_config["task_params"],
            )

            tasks.append(task_instance)

        self._dag = Dag(
            dag_id=dag_id, dag_description=dag_description, tasks=tasks, context=self._context
        )

        return self._dag
