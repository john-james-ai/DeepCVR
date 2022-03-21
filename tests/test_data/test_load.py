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
import pytest
import logging
import inspect

from deepcvr.base.dag import DagBuilder
from deepcvr.utils.config import config_dag


# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.load
class TestLoad:
    def test_load(self, caplog) -> None:
        caplog.set_level(logging.DEBUG)

        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        config_filepath = "tests/test_config/load.yaml"
        mode = "test"
        config = config_dag(config_filepath)[mode]
        # Credentials go to context
        credentials_filepath = "tests/test_config/credentials.yaml"
        credentials = config_dag(credentials_filepath)

        dag = DagBuilder(config=config, context=credentials).build()

        dag.run()

        # tables = ["impressions", "features", "common_feature_groups", "common_features"]
        # dao = DAO(connection_string=credentials["database_uri"][mode])
        # for table in tables:
        #     start = datetime.now()
        #     df = dao.selectall(table_name=table)
        #     duration = datetime.now() - start
        #     msg = "Read {} rows from {} table in {} seconds".format(
        #         str(df.shape[0]), table, str(duration.total_seconds())
        #     )
        #     logger.info(msg)

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
