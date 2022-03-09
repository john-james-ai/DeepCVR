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
# Modified : Wednesday, March 9th 2022, 5:09:34 am                                                 #
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
import shutil
from deepcvr.data.transform import transform

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.transform
class TestTransform:
    def test_transform_core_features(self, caplog) -> None:
        caplog.set_level(logging.INFO)
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree("tests/data/development/transformed", ignore_errors=True)

        data = {
            "train_core": {
                "in_filepath": "tests/data/development/staged/sample_skeleton_train.csv",
                "out_filepath": "tests/data/development/transformed/train_core_features.csv",
                "filetype": "core",
            },
            "test_core": {
                "in_filepath": "tests/data/development/staged/sample_skeleton_test.csv",
                "out_filepath": "tests/data/development/transformed/test_core_features.csv",
                "filetype": "core",
            },
            "train_common": {
                "in_filepath": "tests/data/development/staged/common_features_train.csv",
                "out_filepath": "tests/data/development/transformed/common_features_train.csv",
                "filetype": "common",
            },
            "test_common": {
                "in_filepath": "tests/data/development/staged/common_features_test.csv",
                "out_filepath": "tests/data/development/transformed/common_features_test.csv",
                "filetype": "common",
            },
        }

        for _, v in data.items():
            transform(v["in_filepath"], v["out_filepath"], v["filetype"])

        assert len(os.listdir("tests/data/development/transformed")) == 4, logger.error(
            "Unexpected files in transformed directory"
        )

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
