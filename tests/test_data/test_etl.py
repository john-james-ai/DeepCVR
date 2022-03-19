#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_etl.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/deepcvr                                              #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, March 18th 2022, 8:40:02 pm                                                   #
# Modified : Saturday, March 19th 2022, 5:04:26 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import pytest
import logging
import inspect
import shutil
import warnings

from deepcvr.data.etl import ETL

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.etl
class TestExtract:
    def test_extract(self, caplog) -> None:
        caplog.set_level(logging.INFO)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        filepath = "tests/data"
        shutil.rmtree(filepath, ignore_errors=True)

        task_id = 0
        task_name = "extract_transform_load"
        params = {"dataset": "train", "mode": "development"}
        etl = ETL(task_id=task_id, task_name=task_name, params=params)
        etl.execute()
