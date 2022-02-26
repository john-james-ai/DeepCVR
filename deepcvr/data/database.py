#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /databbase.py                                                                         #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 1:28:55 am                                              #
# Modified : Saturday, February 26th 2022, 8:23:10 am                                              #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import logging
import pymysql
import inspect
from deepcvr.utils.config import MySQLConfig
from deepcvr.data.sql.create_db import (
    CreateDatabase,
    CreateImpressionsTable,
    CreateFeaturesTable,
)

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Builder:
    def __init__(self, credentials: MySQLConfig) -> None:

        self._credentials = credentials

    def connect(self) -> None:
        """Creates a connection to the MySQL Database server"""

        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        self._con = pymysql.connect(
            host=self._credentials.host,
            user=self._credentials.user,
            password=self._credentials.password,
            database=self._credentials.dbname,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
        self._con.autocommit(True)

        logger.debug("\tCompleting {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def _execute(self, query) -> bool:
        """Executes DDL queries that return no value"""

        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        cursor = self._con.cursor()

        try:
            if query.data:
                cursor.execute(query.sql, query.data)
            else:
                cursor.execute(query.sql)
            self._con.commit()
            logger.info("{} Successful".format(query.desc))

        except pymysql.MySQLError as e:
            logger.error(e)
            self.close_connection()
            raise Exception(e)

        finally:
            cursor.close()

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def execute(self, context=None) -> bool:
        """Creates a database and tables."""

        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        self.connect()

        self._execute(CreateDatabase)
        self._execute(CreateImpressionsTable)
        self._execute(CreateFeaturesTable)

        self.close_connection()

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    def close_connection(self) -> None:
        self._con.close()


# %%
