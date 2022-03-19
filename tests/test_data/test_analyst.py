#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_download.py                                                                     #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, February 25th 2022, 4:08:17 pm                                                #
# Modified : Friday, March 18th 2022, 8:36:52 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #

#%%
import logging
import numpy as np
import inspect
import pytest
import pandas as pd
import matplotlib.pyplot as plt


from deepcvr.data.analyst import CategoricalFeatureAnalyst, NumericFeatureAnalyst
from deepcvr.utils.config import config_dag

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.analyst
class TestAnalyst:
    def test_categorical_analyst(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "tests/test_config/credentials.yaml"
        connection_string = config_dag(filepath)["database_uri"]["train"]
        logger.info(connection_string)
        fig, ax = plt.subplots()

        analyst = CategoricalFeatureAnalyst(
            feature_name="121",
            tablename="features",
            connection_string=connection_string,
            ax=ax,
            fig=fig,
        )

        # Get Data
        df = analyst.get_features()
        assert df.shape[0] > 0, logger.error("DataFrame has no data")

        # Get sample
        df = analyst.sample_data()
        assert df.shape[0] == 5, logger.error("DataFrame doesn't have expected number of samples")

        # Properties
        assert analyst.feature_name == "121", logger.error("Invalid feature name in property")
        assert analyst.tablename == "features", logger.error("Invalid table name")
        assert isinstance(analyst.fig, plt.Figure), logger.error("Figure property error")
        assert isinstance(analyst.figsize, list), logger.error("Figsize error. Not a tuple")
        assert isinstance(analyst.get_unique(), np.ndarray), logger.error(
            "Get unique didn't return a list"
        )
        assert isinstance(analyst.get_uniqueness(), float), logger.error(
            "Get uniqueness didnt' return a float"
        )
        assert isinstance(analyst.get_value_counts(), pd.DataFrame), logger.error(
            "Value counts not a dataframe"
        )
        assert isinstance(analyst.get_missing_count(), int), logger.error(
            "Missing count not an int"
        )
        assert isinstance(analyst.get_missingness(), float), logger.error(
            "Get missingness didnt' return a float"
        )

        assert isinstance(analyst.describe(), pd.DataFrame), logger.error(
            "Describe didn't produce a dataframe"
        )

        # Plots
        title = "Some Bar Chart"
        filepath = "tests/figures/barplot"
        analyst.barplot(title=title, filepath=filepath)

        title = "Cumulative Distribution Function"
        filepath = "tests/figures/cfd"
        analyst.cfd(title=title, filepath=filepath)

        title = "Zipf's Plot"
        filepath = "tests/figures/zipf"
        analyst.zipf(title=title, filepath=filepath)

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )

    def test_numeric_analyst(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "tests/test_config/credentials.yaml"
        connection_string = config_dag(filepath)["database_uri"]["train"]
        fig, ax = plt.subplots()

        analyst = NumericFeatureAnalyst(
            feature_name="150_14",
            tablename="features",
            connection_string=connection_string,
            ax=ax,
            fig=fig,
        )

        # Get Data
        df = analyst.get_features()
        assert df.shape[0] > 0, logger.error("DataFrame has no data")

        # Get sample
        df = analyst.sample_data()
        assert df.shape[0] == 5, logger.error("DataFrame doesn't have expected number of samples")

        # Properties
        assert analyst.feature_name == "150_14", logger.error("Invalid feature name in property")
        assert analyst.tablename == "features", logger.error("Invalid table name")
        assert isinstance(analyst.fig, plt.Figure), logger.error("Figure property error")
        assert isinstance(analyst.figsize, list), logger.error("Figsize error. Not a tuple")
        assert isinstance(analyst.get_unique(), np.ndarray), logger.error(
            "Get unique didn't return a list"
        )
        assert isinstance(analyst.get_uniqueness(), float), logger.error(
            "Get uniqueness didnt' return a float"
        )
        assert isinstance(analyst.get_value_counts(), pd.DataFrame), logger.error(
            "Value counts not a dataframe"
        )
        assert isinstance(analyst.get_missing_count(), int), logger.error(
            "Missing count not an int"
        )
        assert isinstance(analyst.get_missingness(), float), logger.error(
            "Get missingness didnt' return a float"
        )

        assert isinstance(analyst.describe(), pd.DataFrame), logger.error(
            "Describe didn't produce a dataframe"
        )

        # Plots
        title = "Histogram"
        filepath = "tests/figures/histogram"
        analyst.histogram(title=title, filepath=filepath)

        title = "Boxplot"
        filepath = "tests/figures/boxplot"
        analyst.boxplot(title=title, filepath=filepath)

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )


if __name__ == "__main__":
    t = TestAnalyst()
    t.test_categorical_analyst()
    t.test_numeric_analyst()
#%%
