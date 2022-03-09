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
# Modified : Tuesday, March 8th 2022, 10:47:52 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Base and abstract class definitions."""
from abc import ABC, abstractmethod
from typing import Any, Type

# ------------------------------------------------------------------------------------------------ #


class Task(ABC):
    """Abstract class for task classes"""

    def __init__(self, config: str, mode: str = "dev") -> None:
        self._mode = "development" if "dev" in mode else "production"
        self._config = config
        try:
            config[self._mode][self.__class__.__name__]
        except KeyError:
            e = "Configuration for {} mode and {} class does not exist".format(
                self._mode, self.__class__.__name__
            )
            raise KeyError(e)

    @abstractmethod
    def execute(self) -> Any:
        pass


# ------------------------------------------------------------------------------------------------ #


class ETLDag(ABC):
    """Abstract base class for ETL directed acyclic graphs

    Args:
        mode (str): Mode is 'dev' for development or 'prod' for production data.
        config_filepath (str): The location of the

    """

    def __init__(self, config: dict, mode: str = "dev") -> None:
        self._config = config
        self._mode = "development" if "dev" in mode else "production"
        self._tasks = []

    def add_task(self, task: Type[Task]) -> None:
        """Adds a task to the DAG

        Args:
            task (Type[Task]): Class. One of the three supported classes
        """
        instance = task(config=self._config, mode=self._mode)
        self._tasks.append(instance)

    def run(self) -> None:
        for task in self._tasks:
            task.execute()
