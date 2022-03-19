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
# Modified : Saturday, March 19th 2022, 4:56:35 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Tasks that complete the Load phase of the ETL DAG"""
import sqlalchemy
from pymysql import connect
from pymysql.cursors import DictCursor
from typing import Any

from deepcvr.data.base import Task
from deepcvr.utils.io import ParquetIO
from deepcvr.utils.decorators import task_event


# ------------------------------------------------------------------------------------------------ #
#                                DATABASE DEFINITION LANGUAGE                                      #
# ------------------------------------------------------------------------------------------------ #
"""Defines the DDL for the ETL process"""
DDL = {}

DDL["foreign_key_checks_off"] = """SET FOREIGN_KEY_CHECKS = 0;"""
DDL["foreign_key_checks_on"] = """SET FOREIGN_KEY_CHECKS = 1;"""

# Drop Foreign Key Constraints
DDL["drop_constraint_features"] = """ALTER TABLE features DROP FOREIGN KEY fk_features;"""
DDL[
    "drop_constraint_common_features"
] = """ALTER TABLE common_features DROP FOREIGN KEY FK_tbl_common_features_ibfk_1;"""


# Drop Databases
DDL["drop_development_train_db"] = """DROP DATABASE IF EXISTS deepcvr_development_train;"""
DDL["drop_development_test_db"] = """DROP DATABASE IF EXISTS deepcvr_development_test;"""
DDL["drop_production_train_db"] = """DROP DATABASE IF EXISTS deepcvr_train;"""
DDL["drop_production_test_db"] = """DROP DATABASE IF EXISTS deepcvr_test;"""

# Create Databases
DDL["create_development_train_db"] = """CREATE DATABASE deepcvr_development_train;"""
DDL["create_development_test_db"] = """CREATE DATABASE deepcvr_development_test;"""
DDL["create_production_train_db"] = """CREATE DATABASE deepcvr_train;"""
DDL["create_production_test_db"] = """CREATE DATABASE deepcvr_test;"""

DDL[
    "add_primary_key_impressions"
] = """
ALTER TABLE impressions ADD PRIMARY KEY(sample_id);
"""

DDL[
    "add_foreign_key_constraint_1"
] = """
ALTER TABLE features
ADD CONSTRAINT fk_features
FOREIGN KEY (sample_id)
REFERENCES impressions(sample_id)
"""

# ------------------------------------------------------------------------------------------------ #
#                                  DATABASE DEFINITION CLASS                                       #
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

    @task_event
    def execute(self, context: Any = None) -> None:

        # Obtain credentials for mysql database from context
        credentials = context["john"]

        for database, statements in self._params.items():
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
                    cursor.execute(DDL[statement])
                connection.commit()

            connection.close()


# ------------------------------------------------------------------------------------------------ #
#                                     DATABASE LOADER                                              #
# ------------------------------------------------------------------------------------------------ #


class DataLoader(Task):
    """Loads data into tables."""

    def __init__(self, task_id: int, task_name: str, params: dict) -> None:
        super(DataLoader, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @task_event
    def execute(self, context: Any = None) -> None:

        io = ParquetIO()

        engine = sqlalchemy.create_engine(
            context["database_uri"][self._params["connection_string"]]
        )

        data = io.load(filepath=self._params["filepath"])

        # Identify columns and convert strings to sqlalchemy datatypes
        columns = []
        dtypes = self._params["dtypes"]
        for column, dtype in self._params["dtypes"].items():
            dtypes[column] = eval(dtype)
            columns.append(column)

        # Extract columns of interest
        data = data[columns]

        data.to_sql(
            name=self._params["table"], con=engine, index=False, if_exists="replace", dtype=dtypes,
        )
