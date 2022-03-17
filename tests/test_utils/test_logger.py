#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_logger.py                                                                       #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, March 14th 2022, 6:42:55 pm                                                   #
# Modified : Monday, March 14th 2022, 11:49:06 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import os
import pytest

from deepcvr.utils.logger import LoggerBuilder

# ---------------------------------------------------------------------------- #


@pytest.mark.logger
class TestLogger:
    def test_console_logger(self) -> None:
        builder = LoggerBuilder()
        logger = builder.build(__name__).set_console_handler().logger
        logger.info("Test Console Logger Successful")
        # Printing below the set logging level
        logger = builder.reset().build("no_name").set_console_handler().set_level("warn").logger
        logger.debug("This should not print")

    def test_file_logger(self) -> None:
        filepath = "tests/logtests.log"
        builder = LoggerBuilder()
        logger = (
            builder.build(__name__)
            .set_file_handler(filepath=filepath)
            .set_level(level="debug")
            .logger
        )
        logger.debug("Test File Logger Successful")

        # Test logging below set level
        logger = builder.reset().build("no_name").set_file_handler().set_level("error").logger
        logger.debug("This should not log")
        assert os.path.exists(filepath)
