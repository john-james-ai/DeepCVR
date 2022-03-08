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
# Created  : Saturday, March 5th 2022, 6:50:21 am                                                  #
# Modified : Saturday, March 5th 2022, 10:54:57 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Adds column names and partition ids to the data and stores the data in the staged directory. """
import os
import logging
from deepcvr.utils.io import load_csv, save_csv
from deepcvr.data import COLS_CORE_DATASET, COLS_COMMON_FEATURES_DATASET

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #


class Stage:
    """Add columns and partition id to the data and stores it in the staged directory

    Args:
        source (str): Input directory
        destination (str): Destination directory
        n_partitions (int): Number of partitions to designate
        force (bool): If True, overwrite existing data in the staged directory.

    """

    def __init__(
        self, source: str, destination: str, n_partitions: int = 20, force: bool = False
    ) -> None:
        """Adds columns and partitions to the dataframes"""
        self._source = source
        self._destination = destination
        self._n_partitions = n_partitions
        self._force = force

    def execute(self) -> None:

        filenames = os.listdir(self._source)

        for filename in filenames:
            source = os.path.join(self._source, filename)
            destination = os.path.join(self._destination, filename)

            if not os.path.exists(destination) or self._force:
                names = self._get_names(destination)

                data = load_csv(source, header=None, names=names)
                reindexed = data.reset_index()

                data["partition"] = reindexed.index % self._n_partitions

                logger.debug(
                    "Staging {} with {} observations".format(destination, str(data.shape[0]))
                )

                save_csv(data, filepath=destination, header=True, index=False)

    def _get_names(self, filename: str) -> list:
        if "common" in filename:
            return COLS_COMMON_FEATURES_DATASET
        else:
            return COLS_CORE_DATASET
