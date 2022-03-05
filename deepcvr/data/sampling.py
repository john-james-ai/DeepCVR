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
# Modified : Saturday, March 5th 2022, 1:08:53 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Multilabel stratified sampling and instance selection"""
import logging
import pandas as pd

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class TaobaoSampler:
    """Stratified sampling with minimum common features constraint"""

    def __init__(
        self,
        core_data: pd.DataFrame,
        common_features_data: pd.DataFrame,
        frac: float = 0.001,
        random_state: int = None,
    ) -> None:
        self._core_data = core_data
        self._common_features_data = common_features_data
        self._frac = frac
        self._random_state = random_state
        self._core_data_sample = None
        self._common_features_data_sample = None

    def execute(self) -> pd.DataFrame:
        """Orchestrates the sampling process and returns the sample."""
        common_feature_counts = self._get_common_features_counts_from_core()
        common_feature_counts = self._add_counts_to_common_features(common_feature_counts)
        common_features_filtered = self._filter_common_features(common_feature_counts)
        core_filtered = self._filter_core(common_features_filtered)
        self._core_data_sample = self._sample_filtered_core(core_filtered)
        self._screen_common_features()
        return self._core_data_sample, self._common_features_data_sample

    @property
    def core_data_sample(self) -> pd.DataFrame:
        return self._core_data_sample

    @property
    def common_features_data_sample(self) -> pd.DataFrame:
        return self._common_features_data_sample

    def _get_common_features_counts_from_core(self) -> pd.DataFrame:
        """Returns value counts for common_feature_index in core dataset"""
        cfc = self._core_data[3].value_counts(normalize=False)
        cfc = cfc.to_frame()
        cfc.reset_index(inplace=True)
        cfc.columns = ["common_feature_index", "count"]
        return cfc

    def _add_counts_to_common_features(self, common_feature_counts: pd.DataFrame) -> pd.DataFrame:
        """Returns common_features with their counts and cumsum in the core dataset"""
        common_with_counts = pd.merge(
            left=self._common_features_data,
            right=common_feature_counts,
            left_on=0,
            right_on="common_feature_index",
        )
        common_with_counts.sort_values(by="count", ascending=False, inplace=True)
        return common_with_counts

    def _filter_common_features(self, common_with_counts: pd.DataFrame) -> pd.DataFrame:
        """Return top self._frac observations from sorte3d common_features (with counts)"""
        filter_by_count = common_with_counts.sort_values(by="count", ascending=False, axis=0)
        # fmt: off
        filter_by_count = filter_by_count[0: int(filter_by_count.shape[0] * self._frac)]
        # fmt: on
        return filter_by_count

    def _filter_core(self, filtered_common_features: pd.DataFrame) -> pd.DataFrame:
        """Filters the core dataset by the filtered common features"""
        core_filtered = pd.merge(
            left=filtered_common_features[0], left_on=0, right=self._core_data, right_on=3
        )
        core_filtered = core_filtered[["0_y", 1, 2, 3, 4, 5]]
        core_filtered.columns = [0, 1, 2, 3, 4, 5]
        return core_filtered

    def _sample_filtered_core(self, core_filtered) -> pd.DataFrame:
        """Returns the sample from the filtered core dataset"""

        # Sample from 'no action' observations
        n = int(
            len(self._core_data.loc[(self._core_data[1] == 0) & (self._core_data[2] == 0)])
            * self._frac
        )
        no_action = core_filtered.loc[(core_filtered[1] == 0) & (core_filtered[2] == 0)].sample(
            n=n, replace=False, random_state=self._random_state
        )

        # Sample clicks
        n = int(
            len(self._core_data.loc[(self._core_data[1] == 1) & (self._core_data[2] == 0)])
            * self._frac
        )
        clicks = core_filtered.loc[(core_filtered[1] == 1) & (core_filtered[2] == 0)].sample(
            n=n, replace=False, random_state=self._random_state
        )

        # Sample conversions
        n = int(
            len(self._core_data.loc[(self._core_data[1] == 1) & (self._core_data[2] == 1)])
            * self._frac
        )
        conversions = core_filtered.loc[(core_filtered[1] == 1) & (core_filtered[2] == 1)].sample(
            n=n, replace=False, random_state=self._random_state
        )

        sample = pd.concat([no_action, clicks], axis=0)
        sample = pd.concat([sample, conversions], axis=0)
        return sample

    def _screen_common_features(self) -> pd.DataFrame:
        """Extracts to common features that exist in the core sample"""
        common_features_sample = pd.merge(
            right=self._core_data_sample[3],
            right_on=3,
            left=self._common_features_data,
            left_on=0,
        )
        self._common_features_data_sample = common_features_sample[[0, 1, 2]].drop_duplicates(
            keep="first", inplace=False, ignore_index=False
        )
