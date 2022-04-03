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
# Modified : Thursday, March 31st 2022, 1:08:51 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Defines the interfaces for classes involved in the construction and implementation of DAGS."""
from abc import ABC, abstractmethod
import pandas as pd
from typing import Any


# ------------------------------------------------------------------------------------------------ #


class Operator(ABC):
    """Abstract class for operator classes

    Args:
        task_id (int): A number, typically used to indicate the sequence of the task within a DAG
        task_name (str): String name
        params (Any): Parameters for the task

    """

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
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
    def execute(self, data: pd.DataFrame = None, context: dict = None) -> Any:
        pass
