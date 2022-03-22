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
# Modified : Monday, March 21st 2022, 8:12:03 pm                                                   #
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

from deepcvr.base.dag import DagBuilder
from deepcvr.utils.config import config_dag


# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.transform
class TestTransform:
    def test_transform(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree("tests/data/preprocessed/", ignore_errors=True)
        shutil.rmtree("tests/data/staged/", ignore_errors=True)

        config_filepath = "tests/test_config/transform.yaml"
        mode = "train"
        config = config_dag(config_filepath)[mode]

        dag = DagBuilder(config=config).build()

        dag.run()

        trainfiles = [
            "tests/data/staged/train/cvr_train.csv",
            "tests/data/staged/train/cvr_core_features_train.csv",
            "tests/data/staged/train/cvr_common_features_train.csv",
        ]

        testfiles = [
            "tests/data/staged/test/cvr_test.csv",
            "tests/data/staged/test/cvr_core_features_test.csv",
            "tests/data/staged/test/cvr_common_features_test.csv",
        ]

        if mode == "train":
            for file in trainfiles:
                assert os.path.exists(file), logger.error("{} is missing".format(file))
        else:
            for file in testfiles:
                assert os.path.exists(file), logger.error("{} is missing".format(file))

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
