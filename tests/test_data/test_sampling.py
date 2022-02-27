#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_sampling.py                                                                     #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 5:39:47 pm                                              #
# Modified : Saturday, February 26th 2022, 8:21:44 pm                                              #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import os
import pytest
import logging
import inspect
import pandas as pd

from deepcvr.data.sampling import MultilabelStratifiedSampler

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.sample
class TestSampler:
    def test_sampler(self) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        frac = 0.0001
        input_core = "data/raw/sample_skeleton_train.csv"
        input_common = "data/raw/common_features_train.csv"
        output_core = "data/sample/sample_skeleton_train.csv"
        output_common = "data/sample/common_features_train.csv"
        random_state = 55

        sampler = MultilabelStratifiedSampler(
            frac=frac,
            input_core_dataset_filepath=input_core,
            input_common_features_filepath=input_common,
            output_core_dataset_filepath=output_core,
            output_common_features_filepath=output_common,
            random_state=random_state,
        )
        df = sampler.sample()
        print(df)

        assert isinstance(df, pd.DataFrame), logger.error("Did not return DataFrame")
        assert os.path.exists(output_core), logger.error("Output core file missing")
        assert os.path.exists(output_common), logger.error("Output common file missing")

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )


if __name__ == "__main__":

    t = TestSampler()
    t.test_sampler()
#%%
