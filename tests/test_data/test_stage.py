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
# Modified : Tuesday, March 8th 2022, 10:12:39 pm                                                  #
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

from deepcvr.data.extract import Stage

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.stage
class TestStage:
    def test_stage(self) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = "tests/data/development/raw/"
        destination = "tests/data/development/staged/"

        extractor = Stage(source=source, destination=destination)
        extractor.execute()

        destination_1 = os.path.join(destination, "sample_skeleton_test.csv")
        assert os.path.exists(destination_1), logger.debug(
            "Failure {} does not exist".format(destination_1)
        )

        destination_2 = os.path.join(destination, "common_features_test.csv")
        assert os.path.exists(destination_2), logger.debug(
            "Failure {} does not exist".format(destination_2)
        )

        destination_3 = os.path.join(destination, "sample_skeleton_train.csv")
        assert os.path.exists(destination_3), logger.debug(
            "Failure {} does not exist".format(destination_3)
        )

        destination_4 = os.path.join(destination, "common_features_train.csv")
        assert os.path.exists(destination_4), logger.debug(
            "Failure {} does not exist".format(destination_4)
        )

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )


#%%
