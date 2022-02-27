#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /sample.py                                                                            #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 1:20:20 pm                                              #
# Modified : Saturday, February 26th 2022, 1:46:22 pm                                              #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Multilabel stratified sampling """
import os
import pandas as pd
from airflow.models.baseoperator import BaseOperator

# ------------------------------------------------------------------------------------------------ #
class MultilabelStratifiedSampler:
    """Performs multilabel stratfied sampling of the ALI-CCP Dataset"""

    def __init__(
        self,
        core_features_filepath: str,
        common_features_filepath: str,
        frac=0.001,
        random_state: int = None,
    ) -> None:
        self._core_features_filepath = core_features_filepath
        self._common_features_filepath = common_features_filepath
        self._frac = frac
        self._random_state = random_state
        self._stats = {
            "impressions": {
                "count": 0,
                "proportion": 0,
            },
            "clicks": {
                "count": 0,
                "proportion": 0,
            },
            "conversions": {
                "count": 0,
                "proportion": 0,
            },
        }

    def _compute_stats(self) -> None:
        """Counts observations and computes proportions for in each stratum"""
