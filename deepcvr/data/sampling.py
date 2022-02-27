#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /sampling.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 2:19:11 pm                                              #
# Modified : Sunday, February 27th 2022, 5:03:16 am                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Multilabel stratified sampling and instance selection"""
import os
import logging
import pandas as pd
import inspect
from tqdm import tqdm

from deepcvr.data import COLS_CORE_DATASET

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class MultilabelStratifiedSampler:
    """Performs sampling according to the frequency distribution of click and conversion labels."""

    def __init__(
        self,
        input_filepath: str,
        output_filepath: str,
        frac: float = 0.001,
        chunk_size: int = 1000000,
        random_state: int = None,
    ) -> None:
        self._input_filepath = input_filepath
        self._output_filepath = output_filepath
        self._frac = frac
        self._chunk_size = chunk_size
        self._random_state = random_state

        self._stats = {
            "total_impressions": 0,
            "no_response": {
                "count": 0,
                "percent": 0,
            },
            "click": {
                "count": 0,
                "percent": 0,
            },
            "conversion": {
                "count": 0,
                "percent": 0,
            },
        }

    def sample(self) -> None:
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        df = self._sample_core_data()
        self._save(df=df, filepath=self._output_core_dataset_filepath)

        common_features = self._sample_common_features(df)
        self._save(df=common_features, filepath=self._output_common_features_filepath)

        stats = self._stats.from_dict(self._stats, orient="columns")

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        return stats

    def sample(self) -> pd.DataFrame:
        """Prepares a dictionary of counts and proportions for each stratum"""

        logger.debug("\t\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        df = self._read_data()
        self._stats["total_impressions"] = df.shape[0]

        df = self._sample_data(data=df, clicks=0, conversions=0)
        self._stats["no_response"]["count"] = df.shape[0]
        self._stats["no_response"]["percent"] = df.shape[0] / self._stats["total_impressions"] * 100
        df = pd.concat([df, df], axis=0)

        df = self._sample_data(data=df, clicks=1, conversions=0)
        self._stats["click"]["count"] = df.shape[0]
        self._stats["click"]["percent"] = df.shape[0] / self._stats["total_impressions"] * 100
        df = pd.concat([df, df], axis=0)

        df = self._sample_data(data=df, clicks=1, conversions=1)
        self._stats["conversion"]["count"] = df.shape[0]
        self._stats["conversion"]["percent"] = df.shape[0] / self._stats["total_impressions"] * 100
        df = pd.concat([df, df], axis=0)

        logger.debug("\t\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        return df

    def _sample_data(self, data: pd.DataFrame, clicks: int, conversions: int) -> pd.DataFrame:
        logger.debug("\t\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        logger.debug("\t\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        return data.loc[
            (data["click_label"] == clicks) & (data["conversion_label"] == conversions)
        ].sample(
            frac=self._frac, replace=False, ignore_index=False, random_state=self._random_state
        )

    def _read_data(self) -> pd.DataFrame:
        """Reads the data"""

        logger.debug("\t\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        names = [
            "click_label",
            "conversion_label",
            "common_features_index",
            "num_features",
            "features_list",
        ]

        rows = sum(1 for _ in open(self._input_core_dataset_filepath, "r"))

        chunks = []

        with tqdm(total=rows, desc="Rows read: ") as bar:
            for chunk in pd.read_csv(
                self._input_core_dataset_filepath,
                header=None,
                index_col=[0],
                chunksize=self._chunk_size,
            ):
                chunks.append(chunk)
                bar.update(len(chunk))

        df = pd.concat((f for f in chunks), axis=0)

        df.columns = COLS_CORE_DATASET
        df.index.name = "sample_id"

        logger.debug("\t\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        return df

    def _read_common_features_data(self) -> pd.DataFrame:
        """Returns core data including labels and sample ids"""

        logger.debug("\t\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        names = [
            "num_features",
            "features_list",
        ]

        rows = sum(1 for _ in open(self._input_common_features_filepath, "r"))

        chunks = []

        with tqdm(total=rows, desc="Rows read: ") as bar:
            for chunk in pd.read_csv(
                self._input_common_features_filepath,
                header=None,
                index_col=[0],
                chunksize=self._chunk_size,
            ):
                chunks.append(chunk)
                bar.update(len(chunk))

        df = pd.concat((f for f in chunks), axis=0)

        df.columns = names
        df.index.name = "common_features_index"

        logger.debug("\t\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        return df

    def _save(self, df: pd.DataFrame, filepath: str) -> None:
        """Saves DataFrame to CSV file."""
        logger.debug("\t\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df.to_csv(filepath)

        logger.debug("\t\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
