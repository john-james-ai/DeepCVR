#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /backend.py                                                                           #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, February 22nd 2022, 6:28:24 am                                               #
# Modified : Tuesday, February 22nd 2022, 9:01:38 am                                               #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from sqlalchemy import create_engine
from airflow.models.baseoperator import BaseOperator
import logging
from deepcvr.utils.config import AirflowBackendConfig

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #


class MySQLBackend(BaseOperator):
    """Establishes a MySQL Database backend for Airflow metadata"""

    def __init__(self, config: AirflowBackendConfig, **kwargs) -> None:
        super(MySQLBackend, self).__init__(**kwargs)
        self._config = config

    def execute(self, context) -> None:
        connection_string = self._config.string
        engine = create_engine(connection_string)
        conn = engine.connect()
        create_db_query = "CREATE DATABASE IF NOT EXISTS airflow_db \
            CHARACTER SET utf8 COLLATE utf8mb4_unicode_ci;"
        drop_user_query = "DROP USER IF EXISTS 'airflow_user'@'localhost"
        create_user_query = "CREATE USER 'airflow_user' IDENTIFIED BY 'airfl0w_pwd';"
        privileges_query = "GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user';"

        try:
            conn.execute(create_db_query)
            conn.execute(drop_user_query)
            conn.execute(create_user_query)
            conn.execute(privileges_query)
        except Exception as e:
            logger.error(e)
            raise Exception(e)
