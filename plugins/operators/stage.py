#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /stage.py                                                                             #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 5th 2022, 8:11:05 am                                                  #
# Modified : Saturday, March 5th 2022, 10:36:08 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Airflow operator stages the raw data. """
from dataclasses import dataclass
from airflow.models.baseoperator import BaseOperator
from deepcvr.data.stage import Stage

# ------------------------------------------------------------------------------------------------ #


@dataclass
class StageConfig:
    task_id: str
    source: str
    destination: str
    n_partitions: int
    force: bool = False


# ------------------------------------------------------------------------------------------------ #


class StageOperator(BaseOperator):
    """Stages the raw data with column names and partition assignments.

    Args:
        source (str): Input filepath
        destination (str): Destination directory
        n_partitions (int): Number of partitions to designate
        force (bool): If True, overwrite existing data in the staged directory.

    """

    def __init__(
        self, source: str, destination: str, n_partitions: int = 20, force: bool = False, **kwargs
    ) -> None:
        super(StageOperator, self).__init__(**kwargs)
        self._stage = Stage(source=source, destination=destination, force=force)

    def execute(self, context) -> None:
        self._stage.execute()
