#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : Deep Learning for Conversion Rate Prediction (CVR)                                    #
# Version  : 0.1.0                                                                                 #
# File     : \sampling.py                                                                          #
# Language : Python 3.7.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, December 27th 2021, 2:03:27 am                                                #
# Modified : Tuesday, February 15th 2022, 9:02:43 am                                               #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
from abc import ABC, abstractmethod
import pandas as pd
from typing import Union

# ------------------------------------------------------------------------------------------------ #


class BaseSampler(ABC):
    """Abstract Base Class for Sampler Classes """

    @abstractmethod
    def sample(self, df: pd.DataFrame) -> None:
        """Returns a sample of the dataset

        Args:
            df (pd.DataFrame): DataFrame containing features and target variables
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class MultilabelStratifiedRandomSampling(BaseSampler):
    """Stratified random sampling in for multilabel datasets

    Args:
        labels (list): List of column names that contain target labels.
        size (float,int): Size of the sample to be returned in terms of
            fraction of the total sample size if size <= 1 or the number of observations if
            size > 1. The default is 0.10.
        shuffle (int): Number of shuffle iterations
        random_state (int): Seed for pseudo-random number generator.
    """

    def __init__(
        self,
        labels: list,
        size: Union[float, int] = 0.1,
        shuffle: int = 5,
        random_state: int = None,
    ) -> None:
        self._labels = labels
        self._size = size
        self._shuffle = shuffle
        self._random_state = random_state

    def sample(self, df: pd.DataFrame) -> pd.DataFrame:
        """Computes statistics for the input dataset

        Args:
            df (pd.DataFrame): DataFrame containing features and target variables
        """
        n = self._size if self._size > 1 else int(len(df) * self._size)

        if self._shuffle is not None:
            df = self._shuffle(df)

        return df.groupby(self._labels).sample(n=n, random_state=self._random_state)

    def _shuffle(self, df: pd.DataFrame) -> pd.DataFrame:
        for i in range(self._shuffle):
            df = df.sample(frac=1)
        return df
