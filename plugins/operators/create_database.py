#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /create_db.py                                                                         #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 8:38:34 am                                              #
# Modified : Saturday, February 26th 2022, 10:34:56 pm                                             #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from airflow.models.baseoperator import BaseOperator

from deepcvr.utils.config import MySQLConfig
from deepcvr.data.database import DeepCVRDb
from deepcvr.data.sql import (
    CreateDatabaseSQL,
    CreateImpressionsTableSQL,
    CreateCoreFeaturesTableSQL,
    CreateCommonFeaturesTableSQL,
)


# ------------------------------------------------------------------------------------------------ #


class CreateDBOperator(BaseOperator):
    """Operator that creates the DeepCVR Database and Tables."""

    def __init__(self, **kwargs) -> None:
        super(CreateDBOperator, self).__init__(**kwargs)
        credentials = MySQLConfig()
        self._db = DeepCVRDb(credentials=credentials)

    def execute(self, context):
        """Creates the database and tables"""
        self._db.connect()

        self._db.execute(CreateDatabaseSQL)
        self._db.execute(CreateImpressionsTableSQL)
        self._db.execute(CreateCoreFeaturesTableSQL)
        self._db.execute(CreateCommonFeaturesTableSQL)

        self._db.close_connection()
