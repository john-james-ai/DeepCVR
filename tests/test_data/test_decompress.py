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
# Modified : Tuesday, March 8th 2022, 9:59:49 pm                                                   #
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

from deepcvr.data.extract import Decompress

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.decompress
class TestDecompress:
    def test_decompress(self) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = "tests/data/development/external/taobao_test.tar.gz"
        destination = "tests/data/development/raw"
        target_1 = os.path.join(destination, "sample_skeleton_test.csv")
        target_2 = os.path.join(destination, "common_features_test.csv")

        decompress = Decompress(source=source, destination=destination)
        decompress.execute()
        assert os.path.exists(target_1), logger.error("Failure. {} is missing".format(target_1))
        assert os.path.exists(target_2), logger.error("Failure. {} is missing".format(target_2))

        source = "tests/data/development/external/taobao_train.tar.gz"
        target_1 = os.path.join(destination, "sample_skeleton_train.csv")
        target_2 = os.path.join(destination, "common_features_train.csv")

        decompress = Decompress(source=source, destination=destination)
        decompress.execute()
        assert os.path.exists(target_1), logger.error("Failure. {} is missing".format(target_1))
        assert os.path.exists(target_2), logger.error("Failure. {} is missing".format(target_2))

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )


if __name__ == "__main__":

    t = TestDecompress()
    t.test_decompress()
#%%
