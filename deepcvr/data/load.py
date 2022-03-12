#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /load.py                                                                              #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 12th 2022, 5:34:59 am                                                 #
# Modified : Saturday, March 12th 2022, 12:30:03 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Tasks that complete the Load phase of the ETL DAG"""
import logging
import sqlalchemy
import inspect
from pymysql import connect
from pymysql.cursors import DictCursor
from typing import Any

from deepcvr.data.core import Task
from deepcvr.utils.io import CsvIO
from deepcvr.data.ddl import DDL

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class DbDefine(Task):
    """Used to create, drop, constrain, adn define databases and tables.

    Args:
        task_id: Sequence number for the task in its dag
        task_name: Human readable name
        params: The parameters the task requires

    """

    def __init__(self, task_id: int, task_name: str, params: dict) -> None:
        super(DbDefine, self).__init__(task_id=task_id, task_name=task_name, params=params)

    def execute(self, context: Any = None) -> None:
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Obtain credentials for mysql database from context
        credentials = context["john"]

        for database, statements in self._params.items():
            logger.debug("\tLogging into {} database".format(database))
            connection = connect(
                host=credentials["host"],
                user=credentials["user"],
                password=credentials["password"],
                database=database,
                charset="utf8",
                cursorclass=DictCursor,
            )

            with connection.cursor() as cursor:
                for statement in statements:
                    logger.debug("\t\tExecuting {}".format(statement))
                    cursor.execute(DDL[statement])
                connection.commit()

            connection.close()

        logger.debug("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


# ------------------------------------------------------------------------------------------------ #


class DataLoader(Task):
    """Loads data into tables."""

    def __init__(self, task_id: int, task_name: str, params: dict) -> None:
        super(DataLoader, self).__init__(task_id=task_id, task_name=task_name, params=params)

    def execute(self, context: Any = None) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        io = CsvIO()

        for database, config in self._params.items():
            logger.debug("\t\tLogging into {} database".format(database))
            engine = sqlalchemy.create_engine(context[config["connection_string"]])

            for table_name, specification in config["tables"].items():
                logger.debug(
                    "\t\tReading {} file from {}.".format(table_name, specification["filepath"])
                )
                data = io.load(filepath=specification["filepath"])

                # Convert strings to sqlalchemy datatypes
                dtypes = specification["dtypes"]
                for column, dtype in dtypes.items():
                    dtypes[column] = eval(dtypes[column])

                logger.debug("\t\tLoading {} table.".format(table_name))
                rows_affected = data.to_sql(
                    name=table_name, con=engine, index=False, if_exists="replace", dtype=dtypes,
                )

                logger.info(
                    "\t\tTable {} in {} is loaded with {} rows.".format(
                        table_name, database, str(rows_affected)
                    )
                )

        logger.info("\tCompleted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
