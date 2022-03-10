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
# Modified : Thursday, March 10th 2022, 12:02:49 am                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #

#%%
import pytest
import logging
import inspect
import shutil
from deepcvr.data.transform import TransformDAGGenerator
from deepcvr.utils.config import config_dag


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

        filepath = "tests/test_config/transform.yaml"
        config = config_dag(filepath)["development"]

        gen = TransformDAGGenerator(config)
        gen.execute()
        for dag in gen.dags:
            dag.run()

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
