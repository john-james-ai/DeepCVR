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
# Modified : Monday, March 21st 2022, 2:02:17 am                                                   #
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


@pytest.mark.extract
class TestExtract:
    def test_extract(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        shutil.rmtree("tests/data/", ignore_errors=True)

        config_filepath = "tests/test_config/extract.yaml"
        config = config_dag(config_filepath)

        dag = DagBuilder(config=config).build()

        dag.run()

        assert len(os.listdir("tests/data/external")) == 2, logger.error(
            "Unexpected files in external directory"
        )

        assert len(os.listdir("tests/data/raw")) == 4, logger.error(
            "Unexpected files in raw directory"
        )

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
