#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /profile.py                                                                           #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, March 4th 2022, 2:32:04 pm                                                    #
# Modified : Friday, March 4th 2022, 8:39:47 pm                                                    #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Profile Data Module"""
from abc import ABC, abstractmethod
import pandas as pd

# ------------------------------------------------------------------------------------------------ #


class AbstractProfiler(ABC):
    """Defines the interface for Profiler Classes"""

    def __init__(self, data: str) -> None:
        self._data = data
        self._stats = None

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @abstractmethod
    def execute(self) -> pd.Series:
        """Computes the statistics, prints and returns as series"""
        pass


# ------------------------------------------------------------------------------------------------ #


class CoreProfiler(AbstractProfiler):
    """Profiles the core datasets"""

    def __init__(self, data: str) -> None:
        super(CoreProfiler, self).__init__(data)

    def execute(self) -> pd.Series:
        stats = {}
        stats["memory"] = self._data.memory_usage(deep=True).sum()
        stats["impressions"] = self._data.shape[0]
        stats["no_action"] = len(self._data.loc[(self._data[1] == 0) & (self._data[2] == 0)])
        stats["clicks"] = self._data[1].sum()
        stats["conversions"] = self._data[2].sum()
        stats["click_through_rate"] = stats["clicks"] / stats["impressions"] * 100
        stats["conversion_rate"] = stats["conversions"] / stats["impressions"] * 100
        stats["unique_common_feature_sets"] = self._data[3].nunique()
        stats["min_features_per_sample"] = self._data[4].min()
        stats["max_features_per_sample"] = self._data[4].max()
        stats["num_na"] = self._data.isna().sum().sum()

        self._stats = pd.Series(stats)
        return self._stats


# ------------------------------------------------------------------------------------------------ #


class CommonFeaturesProfiler(AbstractProfiler):
    """Profiles the core datasets"""

    def __init__(self, data: str) -> None:
        super(CommonFeaturesProfiler, self).__init__(data)

    def execute(self) -> pd.Series:
        stats = {}
        stats["memory"] = self._data.memory_usage(deep=True).sum()
        stats["common_feature_sets"] = self._data.shape[0]

        stats["min_features_per_sample"] = self._data[1].min()
        stats["max_features_per_sample"] = self._data[1].max()
        stats["num_na"] = self._data.isna().sum().sum()

        self._stats = pd.Series(stats)
        return self._stats
