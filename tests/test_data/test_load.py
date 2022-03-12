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
# Modified : Saturday, March 12th 2022, 9:56:22 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #

#%%
import pytest
import logging
import inspect

# from deepcvr.data.database import Database
from deepcvr.data.core import DagBuilder
from deepcvr.utils.config import config_dag


# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.load
class TestTransform:
    def test_load(self, caplog) -> None:
        caplog.set_level(logging.DEBUG)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        config_filepath = "tests/test_config/load.yaml"
        mode = "d"
        config = config_dag(config_filepath)
        # Credentials go to context
        credentials_filepath = "config/credentials.yaml"
        credentials = config_dag(credentials_filepath)

        dag = DagBuilder(config=config, mode=mode, context=credentials).build()

        dag.run()

        # dbs = ["development_train", "development_test"]
        # statements = [
        #     "SELECT * FROM impressions;",
        #     "SELECT * FROM features;",
        #     "SELECT * FROM common_feature_groups;",
        #     "SELECT * FROM common_features;",
        # ]
        # credentials_filepath = "config/credentials.yaml"
        # credentials = config_dag(credentials_filepath)["john"]

        # for database in dbs:
        #     for statement in statements:
        #         db = Database(database=database, credentials=credentials)
        #         rows = db.execute(statement=statement)
        #         assert rows > 100, logger.error(
        #             "Error in {}. Statement {} returned {}.".format(
        # database, statement, str(rows))
        #         )

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
